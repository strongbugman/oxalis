<p>
<a href="https://pypi.org/project/oxalis/">
    <img src="https://badge.fury.io/py/oxalis.svg" alt="Package version">
</a>
</p>

# Oxalis  

Distributed async task/job queue, like Celery for `asyncio` world

## Feature

* Redis and AMQP(RabbitMQ etc.) support
* Task timeout and concurrency limit support
* Delayed task(Both Redis and RabbitMQ) support
* Cron task/job beater
* Built-in coroutine pool with concurrency and time limit

## Install

```pip install oxalis```

## Example with Redis backend

Define task:
```python
from redis.asyncio.client import Redis
from oxalis.redis import Oxalis


oxalis = Oxalis(Redis(host=os.getenv("REDIS_HOST", "redis")))

@oxalis.register()
async def hello_task():
    print("Hello oxalis")
```

Run worker(consumer):
```python
oxalis.run_worker_master()
```

```shell
python ex.py
INFO:oxalis:Registered Task: <Task(hello_task)>
INFO:oxalis:Run worker: <Oxalis(pid-101547)>...
INFO:oxalis:Run worker: <Oxalis(pid-101548)>...
INFO:oxalis:Run worker: <Oxalis(pid-101549)>...
INFO:oxalis:Run worker: <Oxalis(pid-101550)>...
INFO:oxalis:Run worker: <Oxalis(pid-101551)>...
INFO:oxalis:Run worker: <Oxalis(pid-101552)>...
INFO:oxalis:Run worker: <Oxalis(pid-101554)>...
```

Run client(producer):
```python
import asyncio

asyncio.get_event_loop().run_until_complete(oxalis.connect())
for i in range(10):
    asyncio.get_event_loop().run_until_complete(hello_task.delay())
    asyncio.get_event_loop().run_until_complete(hello_task.delay(_delay=1))  # delay execution after 1s
```

Run cron beater:
```python
from oxalis.beater import Beater

beater = Beater(oxalis)

beater.register("*/1 * * * *", hello_task)
beater.run()
```
```shell
python exb.py 
INFO:oxalis:Beat task: <Task(hello_task)> at <*/1 * * * *> ...
```

## TaskCodec

The `TaskCodec` will encode/decode task args, default codec will use `json`

Custom task codec:
```python
from oxalis.base import TaskCodec

class MyTaskCodec(TaskCodec):
    @classmethod
    def encode(
        cls,
        task: Task,
        task_args: tp.Sequence[tp.Any],
        task_kwargs: tp.Dict[str, tp.Any],
    ) -> bytes:
        ...

    @classmethod
    def decode(cls, content: bytes) -> TaskCodec.MESSAGE_TYPE:
        ...



oxalis = Oxalis(Redis(host=os.getenv("REDIS_HOST", "redis")), task_codec=MyTaskCodec())
...
```


## Task pool

Oxalis use one coroutine pool with concurrency limit and timeout limit to run all task

Custom pool:

```python
from redis.asyncio.client import Redis
from oxalis.redis import Oxalis
from oxalis.pool import Pool

oxalis = Oxalis(Redis(host=os.getenv("REDIS_HOST", "redis")), pool=Pool(concurrency=10, timeout=60))
```

* For Redis task, the `queue` will be blocked util `pool` is not fully loaded
* For AMQP task, oxalis use AMQP's QOS to limit worker concurrency(`pool`'s concurrency will be -1 which means the pool's concurrency will not be limited)
* `asyncio.TimeoutError` will be raised if one task is timeout
* Every worker process has owned limited pool


Specified one task timeout limit:
```python
@oxalis.register(queue=custom_queue, timeout=10)
def custom_task():
    print("Hello oxalis")
```

## Custom hook

Oxalis defined some hook API for inherited subclass:
```python
class MyOxalis(Oxalis):
    def on_worker_init():
        # will be called before worker started
        pass

    def on_worker_close():
        # will be called after worker started
        pass
```

Some API can be rewritten or inherited for custom usage, eg:
```python
import sentry_sdk

class MyOxalis(Oxalis):
    async def exec_task(self, task: Task, *task_args, **task_kwargs):
        """
        capture exception to sentry
        """
        try:
            await super().exec_task(task, *task_args, **task_kwargs)
        except Exception as e:
            sentry_sdk.capture_exception(e)
```


## Redis Backend Detail

Oxalis use redis's `list` and `pubsub` structure as a message queue

### Queue

Custom queue:
```python
from oxalis.redis import Queue, PubsubQueue

custom_queue = Queue("custom")
bus_queue = PubsubQueue("bus")
```

Register task:
```python
@oxalis.register(queue=custom_queue)
def custom_task():
    print("Hello oxalis")

@oxalis.register(queue=bus_queue)
def bus_task():
    print("Hello oxalis")
```

* For task producer, the task will send to specified queue when call `task.delay()`
* For task consumer, oxalis will listen those queues and receive task from them

### Concurrency limit

Oxalis using coroutine pool's concurrency limit way, we can set different concurrency limit with specified pool for one task:

```python
@oxalis.register(pool=Pool(concurrency=1))
def custom_task():
    print("Hello oxalis")
```

### Delayed task

Support by redis [zset](https://redis.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-4-task-queues/6-4-2-delayed-tasks/)

##  AMQP Backend Detail


### Custom Queue and Exchange

Oxalis using AMQP's way to define Exchange, Queue and their bindings

```python
import asyncio
import logging
import time

from aio_pika import RobustConnection
from oxalis.amqp import Exchange, ExchangeType, Oxalis, Pool, Queue

e = Exchange("test")
q = Queue("test", durable=False)
e2 = Exchange("testfanout", type=ExchangeType.FANOUT)
q2 = Queue("testfanout", durable=False)


oxalis = Oxalis(RobustConnection("amqp://root:letmein@rabbitmq:5672/"))
oxalis.register_binding(q, e, "test")
oxalis.register_binding(q2, e2, "")
oxalis.register_queues([q, q2])


@oxalis.register(exchange=e, routing_key="test")
async def task1():
    await asyncio.sleep(1)
    print("hello oxalis")


@oxalis.register(exchange=e2)
async def task2():
    await asyncio.sleep(10)
    print("hello oxalis")

```

* For producer, task `oxalis.register`  defined one task message will send to which exchange(by routing key)
* For consumer, `register_queues` defined which queues oxalis will listened
* Task routing defined by bindings

### Concurrency limit

Oxalis use AMQP's QOS to limit worker concurrency(Task's `ack_later` should be true), so coroutine pool's concurrency should not be limited.

Custom queue QOS:
```python
oxalis = Oxalis(RobustConnection("amqp://root:letmein@rabbitmq:5672/"), default_queue=Queue("custom",consumer_prefetch_count=10))
...
fanout_queue = Queue("testfanout", durable=False, consumer_prefetch_count=3)
oxalis.register_queues([fanout_queue])
...
```

### Custom task behavior

Define task how to perform `ack` and `reject` 

```python
# always ack even task failed(raise exception)
@oxalis.register(ack_always=True, reject=False)
async def task2():
    await asyncio.sleep(10)
    print("hello oxalis")

#  reject with requeue when task failed
@oxalis.register(reject_requeue=True)
async def task2():
    await asyncio.sleep(10)
    print("hello oxalis")
```

### Delayed task

Support by RabbitMq's [plugin](https://blog.rabbitmq.com/posts/2015/04/scheduling-messages-with-rabbitmq)
