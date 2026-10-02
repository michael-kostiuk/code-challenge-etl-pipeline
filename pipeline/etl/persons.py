"""Phase 2: one process per group of person files; each runs a uvloop event loop that overlaps
org lookups and bulk requests with the CPU-bound parse/transform work."""
from __future__ import annotations

import asyncio
import logging
from contextlib import aclosing
from itertools import batched
from pathlib import Path
from typing import AsyncIterator, Iterable

import aiohttp
import uvloop

from etl.bulk import BulkSender
from etl.config import Config
from etl.deadletter import DeadLetter
from etl.logs import setup_logging
from etl.metrics import Counters
from etl.orgstore import OrgReader
from etl.procs import run_all, split_files
from etl.reader import read_records
from etl.transform import build_document, referenced_org_ids

Batch = tuple[tuple[int, dict], ...]

logger = logging.getLogger(__name__)


def run_persons(cfg: Config, files: list[Path], ctx, blocks: list[Counters]) -> None:
    run_all(ctx, run_worker, split_files(files, cfg.workers), blocks, cfg, name="person-worker")


def run_worker(cfg: Config, files: list[Path], counters: Counters, worker_id: int) -> None:
    setup_logging(cfg.log_level)
    try:
        uvloop.run(_run(cfg, files, counters, worker_id))
    except Exception:
        logger.exception("worker_failed", extra={"worker": worker_id})
        raise SystemExit(1)


async def _run(cfg: Config, files: list[Path], counters: Counters, worker_id: int) -> None:
    dead_letter = DeadLetter(cfg.out_dir / "dead_letter.ndjson")
    store = OrgReader(cfg.redis_url)
    lag_watch = asyncio.create_task(_watch_loop_lag(counters))
    try:
        connector = aiohttp.TCPConnector(limit=cfg.in_flight)
        timeout = aiohttp.ClientTimeout(total=300)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session, asyncio.TaskGroup() as tasks:
            sender = BulkSender(session, tasks, cfg, dead_letter, counters)
            batches = batched(read_records(files, "malformed_person", dead_letter, counters), cfg.lookup_batch)
            async with aclosing(_with_orgs(batches, store)) as joined:
                async for batch, orgs in joined:
                    await _emit(batch, orgs, sender, counters)
            await sender.flush()
    finally:
        lag_watch.cancel()
        await store.aclose()
        dead_letter.close()


async def _with_orgs(batches: Iterable[Batch], store: OrgReader) -> AsyncIterator[tuple[Batch, dict[int, bytes]]]:
    """Yields each batch with its looked-up orgs; the next batch is already parsed and its lookup in
    flight while the caller transforms this one."""
    pending: tuple[Batch, asyncio.Task] | None = None
    try:
        for batch in batches:
            ids = set().union(*(referenced_org_ids(person) for _, person in batch))
            ready, pending = pending, (batch, asyncio.create_task(store.get_many(ids)))
            await asyncio.sleep(0)  # let the lookup go out before the caller's CPU-bound transform
            if ready is not None:
                yield ready[0], await ready[1]
        if pending is not None:
            yield pending[0], await pending[1]
    finally:
        if pending is not None:
            pending[1].cancel()  # no-op unless the caller stopped early


async def _emit(batch: Batch, orgs: dict[int, bytes], sender: BulkSender, counters: Counters) -> None:
    for person_id, person in batch:
        doc = build_document(person, orgs)
        if doc.unresolved_refs:
            counters.add("unresolved_refs", doc.unresolved_refs)
            counters.add("persons_with_unresolved")
        if doc.has_affiliations:
            counters.add("persons_with_affiliations")
        await sender.add(person_id, doc.body)
    counters.add("persons_read", len(batch))


async def _watch_loop_lag(counters: Counters, interval_s: float = 0.05) -> None:
    """Accumulates how long CPU work kept the event loop from servicing I/O."""
    loop = asyncio.get_running_loop()
    while True:
        started = loop.time()
        await asyncio.sleep(interval_s)
        counters.add("loop_blocked_s", max(0.0, loop.time() - started - interval_s))
