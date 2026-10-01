"""Append-only NDJSON record of inputs the pipeline could not index.

All processes append to the same file. Each record is one unbuffered write under an exclusive
flock, so concurrent writers never interleave lines. Dead letters are rare (0 in the real data),
so the lock costs nothing measurable."""
from __future__ import annotations

import fcntl
from pathlib import Path

import orjson


class DeadLetter:
    def __init__(self, path: Path):
        self.path = path
        self.count = 0
        self._fh = None

    def write(self, kind: str, **fields) -> None:
        if self._fh is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(self.path, "ab", buffering=0)
        fcntl.flock(self._fh, fcntl.LOCK_EX)
        try:
            self._fh.write(orjson.dumps({"kind": kind, **fields}, default=str) + b"\n")
        finally:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
        self.count += 1

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
