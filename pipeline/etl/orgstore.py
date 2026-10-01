"""Org lookup store for the join: org forager_id -> compact org JSON bytes, kept in the compose
`redis` service. Org loaders write through `OrgWriter` in parallel; person workers read through
`OrgReader` with batched MGETs.

LMDB and SQLite lost the store benchmark (EVALUATION.md); scripts/reference/alt_orgstores.py.
"""
from __future__ import annotations

from typing import Collection, Sequence

import redis
import redis.asyncio
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

_RETRY_ERRORS = [RedisConnectionError, RedisTimeoutError]


def _key(org_id: int) -> bytes:
    return b"org:%d" % org_id


class OrgWriter:
    def __init__(self, url: str):
        self._client = redis.Redis.from_url(
            url, retry=Retry(ExponentialBackoff(cap=2.0, base=0.1), 5), retry_on_error=_RETRY_ERRORS
        )

    def clear(self) -> None:
        """Discards every org from a previous run; raises if Redis is unreachable."""
        self._client.flushdb()

    def put_many(self, items: Sequence[tuple[int, bytes]]) -> None:
        self._client.mset({_key(org_id): data for org_id, data in items})

    def close(self) -> None:
        self._client.close()


class OrgReader:
    def __init__(self, url: str):
        self._client = redis.asyncio.Redis.from_url(
            url, retry=AsyncRetry(ExponentialBackoff(cap=2.0, base=0.1), 5), retry_on_error=_RETRY_ERRORS
        )

    async def get_many(self, ids: Collection[int]) -> dict[int, bytes]:
        """The stored orgs among `ids`; unknown ids are absent from the result."""
        if not ids:
            return {}
        ids = list(ids)
        values = await self._client.mget([_key(org_id) for org_id in ids])
        return {org_id: v for org_id, v in zip(ids, values) if v is not None}

    async def aclose(self) -> None:
        await self._client.aclose()
