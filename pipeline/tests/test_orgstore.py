import asyncio
import os

import pytest
import redis

from etl.config import Config
from etl.orgstore import create_store, open_store


def _redis_reachable() -> bool:
    try:
        client = redis.Redis.from_url(Config.from_env().redis_url, socket_connect_timeout=1)
        result = client.ping()
        client.close()
        return result
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        return False


BACKENDS = [
    "sqlite",
    "lmdb",
    pytest.param("redis", marks=pytest.mark.skipif(not _redis_reachable(), reason="redis not reachable")),
]


@pytest.mark.parametrize("backend", BACKENDS)
def test_returns_stored_org_bytes_and_omits_unknown_ids(backend, tmp_path):
    cfg = Config.from_env({**os.environ, "ORG_STORE": backend, "STAGE_DIR": str(tmp_path)})
    writer = create_store(cfg)
    writer.put_many([(1, b'{"a":1}')])
    writer.close()

    # Verify create_store() discards previous contents
    writer = create_store(cfg)
    writer.put_many([(2, b'{"b":2}')])
    writer.close()

    async def read():
        reader = open_store(cfg)
        try:
            return await reader.get_many([1, 2])
        finally:
            await reader.aclose()

    assert asyncio.run(read()) == {2: b'{"b":2}'}
