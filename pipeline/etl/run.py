"""Orchestration: org load -> index setup -> person workers -> finalize -> verify -> metrics."""
from __future__ import annotations

import logging
import multiprocessing as mp
import time
from contextlib import contextmanager

import orjson
from elasticsearch import Elasticsearch

from etl.config import Config
from etl.index_admin import create_index, finalize_index
from etl.logs import setup_logging
from etl.metrics import Counters, ProgressReporter, peak_memory_bytes, totals
from etl.orgload import load_orgs
from etl.persons import run_persons

logger = logging.getLogger(__name__)


def main() -> int:
    cfg = Config.from_env()
    setup_logging(cfg.log_level)
    return run(cfg)


def run(cfg: Config) -> int:
    started = time.monotonic()
    logger.info("run_start", extra={"config": cfg.as_log_fields()})
    try:
        ok = _run(cfg, started)
    except Exception:
        logger.exception("run_failed")
        return 1
    return 0 if ok else 1


@contextmanager
def _phase(name: str, timings: dict[str, float]):
    logger.info("phase_start", extra={"phase": name})
    t = time.monotonic()
    yield
    timings[name] = round(time.monotonic() - t, 2)
    logger.info("phase_end", extra={"phase": name, "duration_s": timings[name]})


def _run(cfg: Config, started: float) -> bool:
    org_files = sorted((cfg.data_dir / "organization").glob("*.json.gz"))
    person_files = sorted((cfg.data_dir / "person").glob("*.json.gz"))
    if cfg.person_files:
        person_files = person_files[: cfg.person_files]
    if not org_files or not person_files:
        raise RuntimeError(f"no input files under {cfg.data_dir}/organization or {cfg.data_dir}/person")
    (cfg.out_dir / "dead_letter.ndjson").unlink(missing_ok=True)
    (cfg.out_dir / "metrics.json").unlink(missing_ok=True)  # a failed run must not leave stale metrics

    ctx = mp.get_context("spawn")  # fresh interpreters: no inherited event loops or connections
    org_blocks = [Counters.shared(ctx) for _ in range(cfg.loaders)]
    person_blocks = [Counters.shared(ctx) for _ in range(cfg.workers)]
    es = Elasticsearch(cfg.es_url, request_timeout=300)
    timings: dict[str, float] = {}
    reporter = ProgressReporter(org_blocks + person_blocks, cfg.progress_interval_s)
    reporter.start()
    try:
        with _phase("org_load", timings):
            load_orgs(cfg, org_files, ctx, org_blocks)
        with _phase("index_setup", timings):
            create_index(es, cfg)
        with _phase("persons", timings):
            run_persons(cfg, person_files, ctx, person_blocks)
        with _phase("finalize", timings):
            es_count = finalize_index(es, cfg)
    finally:
        reporter.stop()

    wall_clock_s = time.monotonic() - started
    counts = totals(org_blocks + person_blocks)
    expected = int(counts["persons_read"] - counts["failed_docs"])
    ok = es_count == expected and counts["failed_docs"] == 0
    peak, peak_source = peak_memory_bytes()
    metrics = {
        "persons_per_s": round(counts["docs_indexed"] / wall_clock_s, 1),
        "persons_phase_per_s": round(counts["docs_indexed"] / timings["persons"], 1),
        "wall_clock_s": round(wall_clock_s, 2),
        "phase_s": timings,
        "es_count": es_count,
        "expected_count": expected,
        "ok": ok,
        "peak_memory_bytes": peak,
        "peak_memory_source": peak_source,
        "counters": {k: round(v, 2) if k.endswith("_s") else int(v) for k, v in counts.items()},
        "config": cfg.as_log_fields(),
    }
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    (cfg.out_dir / "metrics.json").write_bytes(orjson.dumps(metrics, option=orjson.OPT_INDENT_2))
    logger.log(logging.INFO if ok else logging.ERROR, "summary", extra=metrics)
    return ok
