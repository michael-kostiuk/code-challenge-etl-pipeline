"""Org lookup stores for the join: org forager_id -> compact org JSON bytes.

Three interchangeable backends, selected by ORG_STORE and compared by benchmark:
  redis  — separate service; loader processes write in parallel; batched MGET lookups.
  lmdb   — memory-mapped file on the stage volume; single writer.
  sqlite — file on the stage volume; single writer.
"""
from __future__ import annotations

import shutil
import sqlite3
import struct
from pathlib import Path
from typing import Collection, Protocol, Sequence

import lmdb
import redis
import redis.asyncio
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

from etl.config import Config

_REDIS_RETRY_ERRORS = [RedisConnectionError, RedisTimeoutError]


class OrgStore(Protocol):
    parallel_writes: bool

    def put_many(self, items: Sequence[tuple[int, bytes]]) -> None: ...
    async def get_many(self, ids: Collection[int]) -> dict[int, bytes]: ...
    def close(self) -> None: ...
    async def aclose(self) -> None: ...


class RedisOrgStore:
    parallel_writes = True

    def __init__(self, url: str):
        self._url = url
        self._sync: redis.Redis | None = None
        self._async: redis.asyncio.Redis | None = None

    @staticmethod
    def _key(org_id: int) -> bytes:
        return b"org:%d" % org_id

    def _client(self) -> redis.Redis:
        if self._sync is None:
            self._sync = redis.Redis.from_url(
                self._url,
                retry=Retry(ExponentialBackoff(cap=2.0, base=0.1), 5),
                retry_on_error=_REDIS_RETRY_ERRORS,
            )
        return self._sync

    def reset(self) -> None:
        self._client().flushdb()

    def put_many(self, items: Sequence[tuple[int, bytes]]) -> None:
        self._client().mset({self._key(org_id): data for org_id, data in items})

    async def get_many(self, ids: Collection[int]) -> dict[int, bytes]:
        if not ids:
            return {}
        if self._async is None:
            self._async = redis.asyncio.Redis.from_url(
                self._url,
                retry=AsyncRetry(ExponentialBackoff(cap=2.0, base=0.1), 5),
                retry_on_error=_REDIS_RETRY_ERRORS,
            )
        ids = list(ids)
        values = await self._async.mget([self._key(org_id) for org_id in ids])
        return {org_id: v for org_id, v in zip(ids, values) if v is not None}

    def close(self) -> None:
        if self._sync is not None:
            self._sync.close()

    async def aclose(self) -> None:
        if self._async is not None:
            await self._async.aclose()
        self.close()


class LmdbOrgStore:
    parallel_writes = False

    def __init__(self, path: Path, *, writer: bool):
        self._env = lmdb.open(
            str(path),
            map_size=8 << 30,  # sparse upper bound, not an allocation
            readonly=not writer,
            readahead=False,   # random lookups; don't pull neighbouring pages
            sync=False,        # throwaway staging data; durability not needed
            metasync=False,
        )

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

    async def aclose(self) -> None:
        self.close()


class SqliteOrgStore:
    parallel_writes = False

    def __init__(self, path: Path, *, writer: bool):
        if writer:
            self._db = sqlite3.connect(path)
            self._db.executescript(
                "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;"
                "CREATE TABLE IF NOT EXISTS orgs (id INTEGER PRIMARY KEY, data BLOB NOT NULL);"
            )
        else:
            self._db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)

    def put_many(self, items: Sequence[tuple[int, bytes]]) -> None:
        self._db.executemany("INSERT OR REPLACE INTO orgs VALUES (?, ?)", items)
        self._db.commit()

    async def get_many(self, ids: Collection[int]) -> dict[int, bytes]:
        ids = list(ids)
        if not ids:
            return {}
        # Chunk into batches of at most 900 ids to avoid exceeding SQLite's variable limit
        result = {}
        batch_size = 900
        for i in range(0, len(ids), batch_size):
            batch = ids[i : i + batch_size]
            placeholders = ",".join("?" * len(batch))
            result.update(dict(self._db.execute(f"SELECT id, data FROM orgs WHERE id IN ({placeholders})", batch)))
        return result

    def close(self) -> None:
        self._db.close()

    async def aclose(self) -> None:
        self.close()


def _file_path(cfg: Config) -> Path:
    return cfg.stage_dir / {"lmdb": "orgs.lmdb", "sqlite": "orgs.sqlite"}[cfg.org_store]


def create_store(cfg: Config) -> OrgStore:
    """An empty, writable store; anything from a previous run is discarded."""
    if cfg.org_store == "redis":
        store = RedisOrgStore(cfg.redis_url)
        store.reset()
        return store
    if cfg.org_store == "lmdb":
        path = _file_path(cfg)
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True)
        return LmdbOrgStore(path, writer=True)
    if cfg.org_store == "sqlite":
        path = _file_path(cfg)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.unlink(missing_ok=True)
        return SqliteOrgStore(path, writer=True)
    raise ValueError(f"unknown ORG_STORE {cfg.org_store!r}; expected redis, lmdb or sqlite")


def open_store(cfg: Config) -> OrgStore:
    """A handle on the store filled by create_store(); open one per process."""
    if cfg.org_store == "redis":
        return RedisOrgStore(cfg.redis_url)
    if cfg.org_store == "lmdb":
        return LmdbOrgStore(_file_path(cfg), writer=False)
    if cfg.org_store == "sqlite":
        return SqliteOrgStore(_file_path(cfg), writer=False)
    raise ValueError(f"unknown ORG_STORE {cfg.org_store!r}; expected redis, lmdb or sqlite")
