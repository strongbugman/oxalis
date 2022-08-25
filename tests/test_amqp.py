import asyncio
import time

import pytest
from aio_pika import RobustConnection

from oxalis.amqp import Exchange, ExchangeType, Oxalis, Queue


@pytest.mark.asyncio
async def test_amqp():
    app = Oxalis(RobustConnection("amqp://root:letmein@rabbitmq:5672/"))
    e = Exchange(
        "test_exchange1",
        type=ExchangeType.X_DELAYED_MESSAGE,
        arguments={"x-delayed-type": ExchangeType.DIRECT},
    )
    q = Queue("test_queue", durable=False)
    app.register_queues([q])
    app.register_binding(q, e, "test")

    x = 1
    y = 1
    start_ts = time.time()
    end_ts = 0

    @app.register()
    async def task():
        nonlocal x
        x = 2
        return 1

    @app.register(queue=q, exchange=e, routing_key="test", ack_later=True)
    def task2():
        nonlocal y, end_ts
        y = 2
        end_ts = time.time()
        return 1

    async def close():
        await asyncio.sleep(1)
        app.close_worker()

    asyncio.ensure_future(close())
    await app.connect()

    await app.send_task(task)
    app.running = True
    app.on_worker_init()
    app._run_worker()
    await asyncio.sleep(0.3)
    await app.send_task(task2, _delay=0.5)
    await app.work()
    assert x == 2
    assert y == 2
    assert end_ts - start_ts > 0.45
