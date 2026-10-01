import asyncio

import aiohttp
import orjson
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from etl.bulk import BulkFailed, BulkSender
from etl.config import Config
from etl.deadletter import DeadLetter
from etl.metrics import Counters

DOCS = [(1, b'{"n":1}'), (2, b'{"n":2}'), (3, b'{"n":3}')]


class FakeBulkServer:
    """Minimal `_bulk` endpoint. `item_status(doc_id, attempt)` decides each document's outcome."""

    def __init__(self, item_status, fail_first_request=False, answer_delay_s=0.0):
        self.stored: set[str] = set()
        self.attempts: dict[str, int] = {}
        self._answer_delay_s = answer_delay_s
        self._item_status = item_status
        self._fail_next = fail_first_request

    async def handle(self, request: web.Request) -> web.Response:
        if self._fail_next:
            self._fail_next = False
            return web.Response(status=503)
        lines = (await request.read()).splitlines()
        items, errors = [], False
        for doc in lines[1::2]:
            doc_id = str(orjson.loads(doc)["n"])
            attempt = self.attempts[doc_id] = self.attempts.get(doc_id, 0) + 1
            status = self._item_status(doc_id, attempt)
            item = {"_id": doc_id, "status": status}
            if status < 300:
                self.stored.add(doc_id)
            else:
                errors = True
                error_type = "mapper_parsing_exception" if status == 400 else "es_rejected_execution_exception"
                item["error"] = {"type": error_type, "reason": "test"}
            items.append({"index": item})
        await asyncio.sleep(self._answer_delay_s)
        return web.json_response({"errors": errors, "items": items})


async def send_docs(server: FakeBulkServer, tmp_path, timeout_s: float = 300) -> Counters:
    app = web.Application()
    app.router.add_post("/persons/_bulk", server.handle)
    http = TestServer(app)
    await http.start_server()
    # BULK_BYTES=1: every document exceeds the limit, so each goes out as its own request.
    cfg = Config.from_env(
        {"ES_URL": str(http.make_url("")), "BULK_BYTES": "1", "RETRY_BACKOFF_S": "0.01"}
    )
    counters = Counters()
    dead_letter = DeadLetter(tmp_path / "dead.ndjson")
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_s)) as session, \
                asyncio.TaskGroup() as tasks:
            sender = BulkSender(session, tasks, cfg, dead_letter, counters)
            for doc_id, doc in DOCS:
                await sender.add(doc_id, doc)
            await sender.flush()
    finally:
        dead_letter.close()
        await http.close()
    return counters


def test_transient_failures_do_not_lose_documents(tmp_path):
    server = FakeBulkServer(
        lambda doc_id, attempt: 429 if doc_id == "2" and attempt == 1 else 201,
        fail_first_request=True,
    )

    asyncio.run(send_docs(server, tmp_path))

    assert server.stored == {"1", "2", "3"}


def test_rejected_document_is_dead_lettered_and_counted(tmp_path):
    server = FakeBulkServer(lambda doc_id, attempt: 400 if doc_id == "2" else 201)

    counters = asyncio.run(send_docs(server, tmp_path))

    assert server.stored == {"1", "3"}
    assert counters.get("failed_docs") == 1
    [line] = (tmp_path / "dead.ndjson").read_bytes().splitlines()
    record = orjson.loads(line)
    assert (record["kind"], record["forager_id"], record["status"], record["error"]["type"]) == (
        "es_rejected",
        2,
        400,
        "mapper_parsing_exception",
    )


def test_ambiguous_failure_fails_without_resending(tmp_path):
    # The server stores the documents, then answers after the client's 0.2s timeout: the outcome is unknown.
    server = FakeBulkServer(lambda doc_id, attempt: 201, answer_delay_s=1)

    with pytest.raises(ExceptionGroup) as failure:
        asyncio.run(send_docs(server, tmp_path, timeout_s=0.2))

    assert failure.group_contains(BulkFailed, match="outcome unknown")

    assert server.attempts["1"] == 1
    assert all(n == 1 for n in server.attempts.values())
