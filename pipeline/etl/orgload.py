"""Phase 1: parse org files in parallel and fill a fresh org store."""
from __future__ import annotations

import queue as queue_module
from pathlib import Path

import orjson

from etl.config import Config
from etl.deadletter import DeadLetter
from etl.metrics import Counters
from etl.orgstore import create_store, open_store
from etl.procs import raise_if_failed, split_files, wait_all
from etl.reader import MalformedRecord, malformed_fields, parse_envelope, read_lines

ORG_BATCH = 2000  # ~5 MB of org JSON per store write


def load_orgs(cfg: Config, files: list[Path], ctx, blocks: list[Counters]) -> None:
    store = create_store(cfg)  # fails fast if the store is unreachable
    try:
        groups = split_files(files, cfg.loaders)
        if store.parallel_writes:
            procs = [ctx.Process(target=_load_files, args=(cfg, group, None, blocks[i], i), name=f"org-loader-{i}", daemon=True)
                     for i, group in enumerate(groups)]
            for p in procs:
                p.start()
            wait_all(procs)
            return
        # Single-writer backends: loaders parse, this process writes.
        batches = ctx.Queue(maxsize=cfg.loaders * 4)
        procs = [ctx.Process(target=_load_files, args=(cfg, group, batches, blocks[i], i), name=f"org-loader-{i}", daemon=True)
                 for i, group in enumerate(groups)]
        for p in procs:
            p.start()
        try:
            finished = 0
            while finished < len(procs):
                try:
                    batch = batches.get(timeout=1.0)
                except queue_module.Empty:
                    raise_if_failed(procs)
                    continue
                if batch is None:
                    finished += 1
                else:
                    store.put_many(batch)
            wait_all(procs)
        except BaseException:
            for p in procs:
                p.terminate()
            for p in procs:
                p.join()
            raise
    finally:
        store.close()


def _load_files(cfg: Config, files: list[Path], batches, counters: Counters, loader_id: int) -> None:
    dead_letter = DeadLetter(cfg.out_dir / "dead_letter.ndjson")
    store = open_store(cfg) if batches is None else None
    sink = store.put_many if store is not None else batches.put
    batch: list[tuple[int, bytes]] = []
    try:
        for path in files:
            for lineno, line in read_lines(path):
                try:
                    org_id, data = parse_envelope(line)
                except MalformedRecord as err:
                    dead_letter.write("malformed_org", **malformed_fields(path, lineno, line, err))
                    counters.add("malformed")
                    continue
                batch.append((org_id, orjson.dumps(data)))
                if len(batch) >= ORG_BATCH:
                    sink(batch)
                    counters.add("orgs_loaded", len(batch))
                    batch = []
        if batch:
            sink(batch)
            counters.add("orgs_loaded", len(batch))
    finally:
        if batches is not None:
            batches.put(None)  # end-of-stream marker, also sent on failure
        if store is not None:
            store.close()
        dead_letter.close()
