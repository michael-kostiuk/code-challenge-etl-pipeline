"""Async `_bulk` sender: byte-capped batches, bounded concurrency, retries, dead letters.

Request bodies are built from already-serialized documents; the elasticsearch client's bulk
helpers are avoided because they re-serialize every document.

Documents get auto-generated IDs (Elasticsearch's append-only fast path), so a retry is only safe when
nothing was written: clean rejections (429/503, per-item 429/5xx, connection never established) are
retried; any ambiguous failure (timeout, dropped connection, other 5xx) raises `BulkFailed` instead.
"""
from __future__ import annotations

import asyncio
import random
import time

import aiohttp
import orjson

from etl.config import Config
from etl.deadletter import DeadLetter
from etl.metrics import Counters, log

_HEADERS = {"Content-Type": "application/x-ndjson"}


class BulkFailed(RuntimeError):
    """Documents could not be indexed after all retries; the run must fail."""


class _Retryable(Exception):
    pass


def _bulk_body(items: list[tuple[int, bytes]]) -> bytes:
    parts = []
    for doc_id, doc in items:
        parts.append(b'{"index":{}}\n')
        parts.append(doc)
        parts.append(b"\n")
    return b"".join(parts)


class BulkSender:
    def __init__(self, session: aiohttp.ClientSession, cfg: Config, dead_letter: DeadLetter, counters: Counters):
        self._session = session
        self._url = f"{cfg.es_url}/{cfg.index_name}/_bulk"
        self._max_bytes = cfg.bulk_bytes
        self._max_retries = cfg.max_retries
        self._backoff_s = cfg.retry_backoff_s
        self._slots = asyncio.Semaphore(cfg.in_flight)
        self._tasks: set[asyncio.Task] = set()
        self._buffer: list[tuple[int, bytes]] = []
        self._buffered = 0
        self._dead_letter = dead_letter
        self._counters = counters
        self._error: BaseException | None = None

    async def add(self, doc_id: int, doc: bytes) -> None:
        self._raise_if_failed()
        self._buffer.append((doc_id, doc))
        self._buffered += len(doc)
        if self._buffered >= self._max_bytes:
            await self._dispatch()

    async def flush(self) -> None:
        if self._buffer:
            await self._dispatch()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        self._raise_if_failed()

    async def _dispatch(self) -> None:
        items, self._buffer, self._buffered = self._buffer, [], 0
        waited_from = time.monotonic()
        await self._slots.acquire()  # backpressure: at most `in_flight` requests per worker
        self._counters.add("slot_wait_s", time.monotonic() - waited_from)
        task = asyncio.create_task(self._send(items))
        self._tasks.add(task)
        task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        self._slots.release()
        if not task.cancelled() and task.exception() is not None and self._error is None:
            self._error = task.exception()

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise self._error

    async def _send(self, items: list[tuple[int, bytes]]) -> None:
        pending = items
        for attempt in range(self._max_retries + 1):
            if attempt:
                await asyncio.sleep(self._backoff_s * 2 ** (attempt - 1) * (0.5 + random.random()))
            body = _bulk_body(pending)
            try:
                async with self._session.post(self._url, data=body, headers=_HEADERS) as resp:
                    payload = await resp.read()
                    if resp.status in (429, 503):
                        raise _Retryable(f"HTTP {resp.status}")
                    if resp.status >= 500:
                        raise BulkFailed(f"bulk request failed ambiguously: HTTP {resp.status}; not retried with auto IDs")
                    if resp.status >= 400:
                        raise BulkFailed(f"bulk request rejected: HTTP {resp.status}: {payload[:500]!r}")
            except (aiohttp.ClientConnectorError, _Retryable) as err:
                self._counters.add("retries")
                log("bulk_retry", level="warning", attempt=attempt + 1, docs=len(pending), error=str(err))
                continue
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                raise BulkFailed(
                    f"bulk request outcome unknown ({err!r}); not retried because auto-generated IDs could duplicate documents"
                ) from err
            self._counters.add("bytes_sent", len(body))
            pending = self._handle_items(pending, orjson.loads(payload))
            if not pending:
                return
            self._counters.add("rejected_items", len(pending))
        raise BulkFailed(f"{len(pending)} documents still failing after {self._max_retries} retries")

    def _handle_items(self, pending: list[tuple[int, bytes]], response: dict) -> list[tuple[int, bytes]]:
        """Counts successes, dead-letters permanent failures, returns documents worth retrying."""
        if not response.get("errors"):
            self._counters.add("docs_indexed", len(pending))
            return []
        retry = []
        for (doc_id, doc), item in zip(pending, response["items"]):
            result = item["index"]
            status = result["status"]
            if status < 300:
                self._counters.add("docs_indexed")
            elif status == 429 or status >= 500:
                retry.append((doc_id, doc))
            else:
                self._counters.add("failed_docs")
                self._dead_letter.write("es_rejected", forager_id=doc_id, status=status, error=result.get("error"))
        return retry
