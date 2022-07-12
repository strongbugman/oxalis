import asyncio
import typing as tp

from aioredis.client import Redis

from .base import Oxalis as _Oxalis
from .base import Task, TaskCodec, logger
from .pool import Pool


class Queue:
    NAME_PREFIX = "oxalis_queue_"

    def __init__(self, name: str) -> None:
        self.name = self.NAME_PREFIX + name


class PubsubQueue(Queue):
    pass


class Oxalis(_Oxalis):
    def __init__(
        self,
        client: Redis,
        task_codec: TaskCodec = TaskCodec(),
        pool: Pool = Pool(),
        timeout: float = 5.0,
        worker_num: int = 0,
        test: bool = False,
        default_queue_name: str = "default",
    ) -> None:
        super().__init__(
            task_codec=task_codec,
            pool=pool,
            timeout=timeout,
            worker_num=worker_num,
            test=test,
        )
        self.client = client
        self.pubsub = client.pubsub()
        self.queues: tp.Dict[str, Queue] = {}
        self.default_queue = Queue(default_queue_name)

    async def connect(self):
        await self.client.initialize()

    async def disconnect(self):
        await self.client.close()

    async def send_task(self, task: Task, *task_args, **task_kwargs):
        if task.name not in self.tasks:
            raise ValueError(f"Task {task} not register")
        logger.debug(f"Send task {task} to worker...")
        queue = self.queues[task.name]
        content = self.task_codec.encode(task, task_args, task_kwargs)
        if isinstance(queue, PubsubQueue):
            await self.client.publish(queue.name, content)
        else:
            await self.client.rpush(queue.name, content)

    def register(
        self,
        task_name: str = "",
        timeout: float = -1,
        queue: tp.Optional[Queue] = None,
        **_,
    ) -> tp.Callable[[tp.Callable], Task]:
        def wrapped(func):
            task = Task(self, func, name=task_name, timeout=timeout)
            if task.name in self.tasks:
                raise ValueError("double task, check task name")
            self.tasks[task.name] = task
            self.queues[task.name] = queue or self.default_queue
            return task

        return wrapped

    async def _receive_message(self):
        while self.running:
            content = await self.client.blpop(
                list(
                    {
                        q.name
                        for q in self.queues.values()
                        if not isinstance(q, PubsubQueue)
                    }
                ),
                timeout=self.timeout,
            )
            if not content:
                continue
            else:
                await self.on_message_receive(content[1])

    async def _receive_pubsub_message(self):
        while self.running:
            if not self.pubsub.subscribed:
                await self.pubsub.subscribe(
                    *{
                        q.name
                        for q in self.queues.values()
                        if isinstance(q, PubsubQueue)
                    }
                )
            content = await self.pubsub.get_message(
                ignore_subscribe_messages=True, timeout=self.timeout
            )
            if not content:
                continue
            else:
                await self.on_message_receive(content["data"])

    def _run_worker(self):
        if [q for q in self.queues.values() if isinstance(q, PubsubQueue)]:
            asyncio.ensure_future(self._receive_pubsub_message())
        if [q for q in self.queues.values() if not isinstance(q, PubsubQueue)]:
            asyncio.ensure_future(self._receive_message())
