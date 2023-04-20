import asyncio
import os
import time

import pytest

from oxalis.kafka import Oxalis, Pool


@pytest.mark.asyncio
async def test_redis():
    app = Oxalis(f"{os.getenv('KAFKA_HOST', 'kafka')}:9092")
    limit_pool = Pool(concurrency=1)
    await app.connect()
    x = 1
    y = 1
    end_ts = 0

    @app.register(pool=limit_pool)
    async def task():
        nonlocal x
        await asyncio.sleep(0.1)
        x = 2
        return 1

    @app.register()
    async def _():
        return 1

    @app.register(topic="test_topic_2")
    async def task2():
        nonlocal y, end_ts
        end_ts = time.time()
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
