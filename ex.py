# from oxalis.redis import App, Pool, FanOutQueue
from oxalis.amqp import App, Exchange, Queue
from oxalis.cli import run_worker
# from aioredis import Redis
import asyncio
import logging
logging.basicConfig(level=logging.INFO)

# redis = Redis(host="redis")
# app = App(redis, pool=Pool(limit=3, timeout=1))
# q = FanOutQueue("fanout")

app = App("amqp://root:letmein@rabbitmq:5672/")

@app.register(task_name="hello")
async def hello():
    print("hello")
    await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(app.connect())
    for i in range(10):
        asyncio.get_event_loop().run_until_complete(app.send_task(hello))
    run_worker(app)