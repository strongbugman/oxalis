import asyncio
import os

import pytest
from aioredis import Redis

from oxalis.redis import Oxalis, Pool, PubsubQueue

redis = Redis(host=os.getenv("REDIS_HOST", "redis"))


@pytest.fixture(autouse=True)
async def _redis():
    await redis.flushdb()


@pytest.mark.asyncio
async def test_redis():
    app = Oxalis(redis)
    limit_pool = Pool(concurrency=1)
    await app.connect()
    pubsub_queue = PubsubQueue("fanout")
    x = 1
    y = 1

    @app.register(pool=limit_pool)
    async def task():
        nonlocal x
        await asyncio.sleep(0.1)
        x = 2
        return 1

    @app.register()
    async def _():
        return 1

    @app.register(queue=pubsub_queue)
    async def task2():
        nonlocal y
        y = 2
        return 1

    async def close():
        await asyncio.sleep(1)
        app.close_worker()

    asyncio.ensure_future(close())

    for _ in range(10):
        await app.send_task(task)
    app.running = True
    app._run_worker()
    await asyncio.sleep(0.3)
    await app.send_task(task2)
    await app.work()
    assert x == 2
    assert y == 2

    app.test = True
    assert await task.delay() == 1
