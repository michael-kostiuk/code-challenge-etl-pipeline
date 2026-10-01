"""Phase 2: one process per group of person files; each runs a uvloop event loop that overlaps
org lookups and bulk requests with the CPU-bound parse/transform work."""
from __future__ import annotations

import asyncio
from itertools import batched
from pathlib import Path

import aiohttp
import uvloop

from etl.bulk import BulkSender
from etl.config import Config
from etl.deadletter import DeadLetter
from etl.metrics import Counters, log
from etl.orgstore import OrgReader
from etl.procs import run_all, split_files
from etl.reader import read_records
from etl.transform import build_document, referenced_org_ids

Batch = tuple[tuple[int, dict], ...]


def run_persons(cfg: Config, files: list[Path], ctx, blocks: list[Counters]) -> None:
    run_all(ctx, run_worker, split_files(files, cfg.workers), blocks, cfg, name="person-worker")


def run_worker(cfg: Config, files: list[Path], counters: Counters, worker_id: int) -> None:
    try:
        uvloop.run(_run(cfg, files, counters, worker_id))
    except Exception as err:
        log("worker_failed", level="error", worker=worker_id, error=repr(err))
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
            pending = None
            for batch in batched(read_records(files, "malformed_person", dead_letter, counters), cfg.lookup_batch):
                ids: set[int] = set()
                for _, person in batch:
                    ids |= referenced_org_ids(person)
                lookup = asyncio.ensure_future(store.get_many(ids))
                await asyncio.sleep(0)  # let the lookup go out before parsing the next batch
                if pending is not None:
                    await _emit(*pending, sender, counters)
                pending = (batch, lookup)
            if pending is not None:
                await _emit(*pending, sender, counters)
            await sender.flush()
    finally:
        lag_watch.cancel()
        await store.aclose()
        dead_letter.close()


async def _emit(batch: Batch, lookup: asyncio.Future, sender: BulkSender, counters: Counters) -> None:
    orgs = await lookup
    for person_id, person in batch:
        doc, unresolved = build_document(person, orgs)
        if unresolved:
            counters.add("unresolved_refs", unresolved)
            counters.add("persons_with_unresolved")
        if "affiliations" in person:
            counters.add("persons_with_affiliations")
        await sender.add(person_id, doc)
    counters.add("persons_read", len(batch))


async def _watch_loop_lag(counters: Counters, interval_s: float = 0.05) -> None:
    """Accumulates how long CPU work kept the event loop from servicing I/O."""
    loop = asyncio.get_running_loop()
    while True:
        started = loop.time()
        await asyncio.sleep(interval_s)
        counters.add("loop_blocked_s", max(0.0, loop.time() - started - interval_s))
