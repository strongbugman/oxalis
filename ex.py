from lotus.base import RedisLotus, Pool, FanOutQueue
from lotus.cli import run_worker
from aioredis import Redis
import asyncio
import logging
logging.basicConfig(level=logging.DEBUG)


redis = Redis(host="redis")
app = RedisLotus(redis, pool=Pool(limit=3, timeout=1))
q = FanOutQueue("fanout")

@app.register(queue=q, task_name="hello")
async def hello():
    try:
        print("hello")
        await asyncio.sleep(3)
    except asyncio.CancelledError as e:
        await asyncio.sleep(1)
        print("canceled")

if __name__ == "__main__":
    # for i in range(10):
    #     asyncio.get_event_loop().run_until_complete(app.send_task(hello))
    run_worker(app)