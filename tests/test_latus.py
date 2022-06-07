import os
import asyncio

from aioredis import Redis
import pytest

from lotus.base import Lotus, RedisLotus, Pool, FanOutQueue


redis = Redis(host=os.getenv("REDIS_HOST", "redis"))


@pytest.fixture(autouse=True)
async def _redis():
    await redis.flushdb()


@pytest.mark.asyncio
async def test_redis():
    lotus = RedisLotus(redis, pool=Pool())
    fanout_queue = FanOutQueue("fanout")
    x = 1
    y = 1

    @lotus.register()
    def task():
        nonlocal x
        x = 2
        return 1

    @lotus.register(queue=fanout_queue)
    def task2():
        nonlocal y
        y = 2
        return 1
    
    async def close():
        await asyncio.sleep(1)
        lotus.close_worker()

    asyncio.ensure_future(close())

    await lotus.send_task(task)
    lotus.running = True
    lotus.on_worker_init()
    asyncio.ensure_future(lotus._receive_fanout_message())
    asyncio.ensure_future(lotus._receive_message())
    await asyncio.sleep(0.3)
    await lotus.send_task(task2)
    await lotus._run_worker()
    assert x == 2
    assert y == 2
