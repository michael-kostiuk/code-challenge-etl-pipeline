"""Structured logging, cross-process counters and resource metrics."""
from __future__ import annotations

import array
import resource
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import orjson

FIELDS = (
    "orgs_loaded",
    "persons_read",
    "docs_indexed",
    "bytes_sent",
    "malformed",
    "unresolved_refs",
    "persons_with_unresolved",
    "persons_with_affiliations",
    "retries",
    "rejected_items",
    "failed_docs",
    "loop_blocked_s",
    "slot_wait_s",
)
_INDEX = {name: i for i, name in enumerate(FIELDS)}


def log(event: str, level: str = "info", **fields) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "level": level,
        "event": event,
        **fields,
    }
    sys.stdout.write(orjson.dumps(record, default=str).decode() + "\n")
    sys.stdout.flush()


class Counters:
    """One process's counters. `shared()` places them in shared memory so the parent can
    read them; each block has exactly one writing process, so no locking is needed."""

    def __init__(self, values=None):
        self._v = values if values is not None else array.array("d", [0.0] * len(FIELDS))

    @classmethod
    def shared(cls, ctx) -> "Counters":
        return cls(ctx.RawArray("d", len(FIELDS)))

    def add(self, field: str, n: float = 1.0) -> None:
        self._v[_INDEX[field]] += n

    def get(self, field: str) -> float:
        return self._v[_INDEX[field]]

    def snapshot(self) -> dict[str, float]:
        return {name: self._v[i] for i, name in enumerate(FIELDS)}


def totals(blocks: Iterable[Counters]) -> dict[str, float]:
    out = dict.fromkeys(FIELDS, 0.0)
    for block in blocks:
        for name, value in block.snapshot().items():
            out[name] += value
    return out


class ProgressReporter(threading.Thread):
    """Logs aggregated counters and indexing rate every `interval_s` seconds."""

    def __init__(self, blocks: list[Counters], interval_s: float):
        super().__init__(name="progress", daemon=True)
        self._blocks = blocks
        self._interval_s = interval_s
        self._stopped = threading.Event()

    def run(self) -> None:
        started = last_t = time.monotonic()
        last_docs = 0.0
        while not self._stopped.wait(self._interval_s):
            t = totals(self._blocks)
            now = time.monotonic()
            log(
                "progress",
                orgs_loaded=int(t["orgs_loaded"]),
                persons_indexed=int(t["docs_indexed"]),
                rate_now=round((t["docs_indexed"] - last_docs) / (now - last_t)),
                rate_avg=round(t["docs_indexed"] / (now - started)),
                mb_sent=round(t["bytes_sent"] / 2**20),
                retries=int(t["retries"]),
                rejected_items=int(t["rejected_items"]),
                failed_docs=int(t["failed_docs"]),
                unresolved_refs=int(t["unresolved_refs"]),
                loop_blocked_s=round(t["loop_blocked_s"], 1),
                slot_wait_s=round(t["slot_wait_s"], 1),
            )
            last_t, last_docs = now, t["docs_indexed"]

    def stop(self) -> None:
        self._stopped.set()
        self.join()


def peak_memory_bytes() -> tuple[int, str]:
    """Peak memory of this container (cgroup v2), else the largest single-process RSS."""
    try:
        return int(Path("/sys/fs/cgroup/memory.peak").read_text()), "cgroup"
    except (OSError, ValueError):
        kb = max(
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
        )
        return kb * 1024, "max_process_rss"
