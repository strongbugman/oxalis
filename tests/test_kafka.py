import asyncio
import os
import time

import pytest

from oxalis.kafka import Oxalis, Topic


@pytest.mark.asyncio
async def test_kafka():
    topic = Topic(name="test1")
    topic2 = Topic(name="test1", pause=True, enable_auto_commit=False)
    app = Oxalis(
        f"{os.getenv('KAFKA_HOST', 'kafka')}:9092",
        default_topic=topic,
    )
    await app.connect()
    x = 1
    y = 1
    end_ts = 0

    @app.register()
    async def task():
        nonlocal x
        await asyncio.sleep(0.1)
        x = 2

    @app.register()
    async def _():
        pass

    @app.register(topic=topic2)
    def task2():
        nonlocal y, end_ts
        end_ts = time.time()
        y = 2

    async def close():
        await asyncio.sleep(5)
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
    assert await task.delay() is None
