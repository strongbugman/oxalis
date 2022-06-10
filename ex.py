# from oxalis.redis import App, Pool, FanOutQueue
from oxalis.amqp import App, Exchange, Queue, ExchangeType
from oxalis.cli import run_worker
# from aioredis import Redis
import asyncio
import logging
logging.basicConfig(level=logging.INFO)

# redis = Redis(host="redis")
# app = App(redis, pool=Pool(limit=3, timeout=1))
# q = FanOutQueue("fanout")


e = Exchange("test")
q = Queue("test")
e2 = Exchange("testfanout", type=ExchangeType.FANOUT)
q2 = Queue("testfanout")
q22 = Queue("testfanout2")


app = App("amqp://root:letmein@rabbitmq:5672/")
app.register_binding(q, e, "test")
app.register_binding(q2, e2, "")
app.register_binding(q22, e2, "")
app.register_queues([q])


@app.register(task_name="hello", exchange=e, routing_key="test", ack_later=True)
async def hello():
    print("hello")
    await asyncio.sleep(1)
    # raise ValueError()


@app.register(task_name="hello2", exchange=e2, routing_key="test")
async def hello2():
    print("hello2")
    await asyncio.sleep(1)


@app.register(task_name="hello3", exchange=e2, routing_key="test")
async def hello3():
    print("hello3")
    await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(app.connect())
    for i in range(10):
        asyncio.get_event_loop().run_until_complete(hello.delay())
    run_worker(app)
