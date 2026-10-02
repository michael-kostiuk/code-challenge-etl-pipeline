"""Phase 1: parse org files in parallel processes, each writing its orgs to the org store."""
from __future__ import annotations

from itertools import batched
from pathlib import Path

import orjson

from etl.config import Config
from etl.deadletter import DeadLetter
from etl.logs import setup_logging
from etl.metrics import Counters
from etl.orgstore import OrgWriter
from etl.procs import run_all, split_files
from etl.reader import read_records

ORG_BATCH = 2000  # ~5 MB of org JSON per store write


def load_orgs(cfg: Config, files: list[Path], ctx, blocks: list[Counters]) -> None:
    store = OrgWriter(cfg.redis_url)
    try:
        store.clear()  # fails fast if the store is unreachable
    finally:
        store.close()
    run_all(ctx, _load_files, split_files(files, cfg.loaders), blocks, cfg, name="org-loader")


def _load_files(cfg: Config, files: list[Path], counters: Counters, loader_id: int) -> None:
    setup_logging(cfg.log_level)
    dead_letter = DeadLetter(cfg.out_dir / "dead_letter.ndjson")
    store = OrgWriter(cfg.redis_url)
    try:
        records = read_records(files, "malformed_org", dead_letter, counters)
        for batch in batched(((org_id, orjson.dumps(data)) for org_id, data in records), ORG_BATCH):
            store.put_many(batch)
            counters.add("orgs_loaded", len(batch))
    finally:
        store.close()
        dead_letter.close()
