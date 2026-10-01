"""LMDB and SQLite org stores: benchmark evidence only. NOT wired into the pipeline.

The pipeline's org store is Redis (pipeline/etl/orgstore.py). These two backends were the
alternatives in the org-store benchmark (EVALUATION.md, Trade-offs), full data:

  Redis 6,142 p/s (org load 12.1 s) · LMDB 1,620 p/s (76.1 s) · SQLite 1,350 p/s (206.0 s)

LMDB and SQLite also exceeded the 2 GiB pipeline memory gate: the page cache of the mapped/db
file is charged to the container's cgroup.

They were last wired in commit 0f1869e, selected by ORG_STORE=lmdb|sqlite, with files on a
`stage` volume (STAGE_DIR). Neither supports concurrent writers, so the org loaders parsed and
sent batches over a multiprocessing queue and the parent process was the single writer
(`load_orgs_single_writer` below). `get_many` is `async` only to match the Redis interface; it
blocks, so the person workers' prefetch overlapped nothing for these backends.

Kept so the comparison can be read; this module is not imported, tested or maintained. To re-run
the benchmark, check out 0f1869e.
"""
from __future__ import annotations

import queue as queue_module
import shutil
import sqlite3
import struct
from pathlib import Path
from typing import Collection, Sequence

import lmdb  # was pinned as lmdb==1.5.1


class LmdbOrgStore:
    def __init__(self, path: Path, *, writer: bool):
        self._env = lmdb.open(
            str(path),
            map_size=8 << 30,  # sparse upper bound, not an allocation
            readonly=not writer,
            readahead=False,   # random lookups; don't pull neighbouring pages
            sync=False,        # throwaway staging data; durability not needed
            metasync=False,
        )

    @classmethod
    def create(cls, path: Path) -> "LmdbOrgStore":
        """An empty, writable store; anything from a previous run is discarded."""
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True)
        return cls(path, writer=True)

    @staticmethod
    def _key(org_id: int) -> bytes:
        return struct.pack(">Q", org_id)

    def put_many(self, items: Sequence[tuple[int, bytes]]) -> None:
        with self._env.begin(write=True) as txn:
            txn.cursor().putmulti([(self._key(org_id), data) for org_id, data in items])

    async def get_many(self, ids: Collection[int]) -> dict[int, bytes]:
        out = {}
        with self._env.begin() as txn:
            for org_id in ids:
                value = txn.get(self._key(org_id))
                if value is not None:
                    out[org_id] = value
        return out

    def close(self) -> None:
        self._env.close()


class SqliteOrgStore:
    _MAX_VARIABLES = 900  # SQLite's per-statement bound-parameter limit is 999 on older builds

    def __init__(self, path: Path, *, writer: bool):
        if writer:
            self._db = sqlite3.connect(path)
            self._db.executescript(
                "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;"
                "CREATE TABLE IF NOT EXISTS orgs (id INTEGER PRIMARY KEY, data BLOB NOT NULL);"
            )
        else:
            self._db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)

    @classmethod
    def create(cls, path: Path) -> "SqliteOrgStore":
        """An empty, writable store; anything from a previous run is discarded."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.unlink(missing_ok=True)
        return cls(path, writer=True)

    def put_many(self, items: Sequence[tuple[int, bytes]]) -> None:
        self._db.executemany("INSERT OR REPLACE INTO orgs VALUES (?, ?)", items)
        self._db.commit()

    async def get_many(self, ids: Collection[int]) -> dict[int, bytes]:
        ids = list(ids)
        result = {}
        for i in range(0, len(ids), self._MAX_VARIABLES):
            chunk = ids[i : i + self._MAX_VARIABLES]
            placeholders = ",".join("?" * len(chunk))
            result.update(dict(self._db.execute(f"SELECT id, data FROM orgs WHERE id IN ({placeholders})", chunk)))
        return result

    def close(self) -> None:
        self._db.close()


def load_orgs_single_writer(store, procs, batches) -> None:
    """The parent's side of the single-writer org load. `procs` are already-started loader
    processes that put lists of (org_id, org_json_bytes) on the `batches` queue and one `None`
    each when done (also on failure)."""
    try:
        finished = 0
        while finished < len(procs):
            try:
                batch = batches.get(timeout=1.0)
            except queue_module.Empty:
                if failed := [p for p in procs if p.exitcode not in (None, 0)]:
                    raise RuntimeError(", ".join(f"{p.name} exited with code {p.exitcode}" for p in failed))
                continue
            if batch is None:
                finished += 1
            else:
                store.put_many(batch)
        for p in procs:
            p.join()
    except BaseException:
        for p in procs:
            p.terminate()
        for p in procs:
            p.join()
        raise
    finally:
        store.close()
