import asyncio

import pytest
import redis

from etl.config import Config
from etl.orgstore import OrgReader, OrgWriter

REDIS_URL = Config.from_env().redis_url


def _redis_reachable() -> bool:
    try:
        client = redis.Redis.from_url(REDIS_URL, socket_connect_timeout=1)
        result = client.ping()
        client.close()
        return result
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        return False


@pytest.mark.skipif(not _redis_reachable(), reason="redis not reachable")
def test_returns_stored_org_bytes_and_omits_cleared_and_unknown_ids():
    writer = OrgWriter(REDIS_URL)
    writer.put_many([(1, b'{"a":1}')])
    writer.clear()
    writer.put_many([(2, b'{"b":2}')])
    writer.close()

    async def read():
        reader = OrgReader(REDIS_URL)
        try:
            return await reader.get_many([1, 2, 3])
        finally:
            await reader.aclose()

    assert asyncio.run(read()) == {2: b'{"b":2}'}
