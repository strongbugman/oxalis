from __future__ import annotations

import asyncio
import typing as tp
from collections import defaultdict

from aioredis.client import Redis

from .base import Oxalis as _Oxalis
from .base import Task as _Task
from .base import TaskCodec, logger
from .pool import Pool


class Queue:
    NAME_PREFIX = "oxalis_queue_"

    def __init__(self, name: str) -> None:
        self.name = self.NAME_PREFIX + name


class PubsubQueue(Queue):
    pass


class Task(_Task):
    def __init__(
        self,
        oxalis: Oxalis,
        func: tp.Callable,
        queue: Queue,
        name="",
        timeout: float = -1,
        pool: tp.Optional[Pool] = None,
    ) -> None:
        super().__init__(oxalis, func, name, timeout, pool)
        self.queue = queue


class Oxalis(_Oxalis):
    def __init__(
        self,
        client: Redis,
        task_cls: tp.Type[Task] = Task,
        task_codec: TaskCodec = TaskCodec(),
        pool: Pool = Pool(),
        timeout: float = 2.0,
        worker_num: int = 0,
        test: bool = False,
        default_queue_name: str = "default",
    ) -> None:
        super().__init__(
            task_cls=task_cls,
            task_codec=task_codec,
            pool=pool,
            timeout=timeout,
            worker_num=worker_num,
            test=test,
        )
        self.client = client
        self.pubsub = client.pubsub()
        self.default_queue = Queue(default_queue_name)
        self._receiving = False

    async def connect(self):
        await self.client.initialize()

    async def wait_close(self):
        while self.pubsub.connection is not None:
            await asyncio.sleep(self.timeout)
        while self._receiving:
            await asyncio.sleep(self.timeout)

    async def disconnect(self):
        await self.client.close()

    async def send_task(self, task: Task, *task_args, _delay: float = 0, **task_kwargs):  # type: ignore[override]
        if task.name not in self.tasks:
            raise ValueError(f"Task {task} not register")
        logger.debug(f"Send task {task} to worker...")
        content = self.task_codec.encode(task, task_args, task_kwargs)
        if isinstance(task.queue, PubsubQueue):
            await self.client.publish(task.queue.name, content)
        else:
            await self.client.rpush(task.queue.name, content)

    def register(
        self,
        *,
        task_name: str = "",
        timeout: float = -1,
        pool: tp.Optional[Pool] = None,
        queue: tp.Optional[Queue] = None,
        **_,
    ) -> tp.Callable[[tp.Callable], Task]:
        def wrapped(func):
            task = self.task_cls(
                self,
                func,
                queue or self.default_queue,
                name=task_name,
                timeout=timeout,
                pool=pool,
            )
            self.register_task(task)
            return task

        return wrapped

    async def _receive_message(self, queue_names: tp.Set[str]):
        self._receiving = True
        while self.running:
            content = await self.client.blpop(list(queue_names), timeout=self.timeout)
            if not content:
                continue
            else:
                await self.on_message_receive(content[1])
        self._receiving = False

    async def _receive_pubsub_message(self, queue_names: tp.Set[str]):
        while self.running:
            if not self.pubsub.subscribed:
                await self.pubsub.subscribe(*queue_names)
            content = await self.pubsub.get_message(
                ignore_subscribe_messages=True, timeout=self.timeout
            )
            if not content:
                continue
            else:
                await self.on_message_receive(content["data"])
        await self.pubsub.close()

    def _run_worker(self):
        """
        Limit queue consume concurrency by pool
        """
        queue_names = defaultdict(set)
        pubsub_queue_names = defaultdict(set)
        for t in self.tasks.values():
            if isinstance(t.queue, PubsubQueue):
                pubsub_queue_names[id(t.pool)].add(t.queue.name)
            else:
                queue_names[id(t.pool)].add(t.queue.name)
        for _, ns in queue_names.items():
            asyncio.ensure_future(self._receive_message(ns))
        for _, ns in pubsub_queue_names.items():
            asyncio.ensure_future(self._receive_pubsub_message(ns))
