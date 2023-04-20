from __future__ import annotations

import asyncio
import time
import typing as tp
import uuid
from collections import defaultdict

from redis.asyncio.client import Redis

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
        delay_queue_name: str = "delay_default",
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
        self.delay_queue = Queue(delay_queue_name)
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
        content = self.task_codec.encode(task, task_args, task_kwargs)
        if _delay:
            logger.debug(f"Send task {task} to worker with {_delay}s delay...")
            await self.client.zadd(
                self.delay_queue.name,
                {uuid.uuid1().bytes + content: time.time() + _delay},
            )
        else:
            logger.debug(f"Send task {task} to worker...")
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

    async def _schedule_delayed_message(
        self, fetch_count: int = 100, time_offset: float = 0.05
    ):
        while self.running:
            now_ts = time.time()
            count = 0
            for content, _ in await self.client.zrangebyscore(
                self.delay_queue.name,
                min=0,
                max=now_ts + time_offset,
                start=0,
                num=fetch_count,
                withscores=True,
            ):
                count += 1
                if await self.client.zrem(
                    self.delay_queue.name, content
                ):  # avoid double schedule
                    try:
                        content = content[16:]
                        task_name, _, _ = self.task_codec.decode(content)
                        if task_name not in self.tasks:
                            logger.warning(f"Received task {task_name} not found")
                        else:
                            task = self.tasks[task_name]
                            if isinstance(task.queue, PubsubQueue):  # type: ignore
                                await self.client.publish(task.queue.name, content)  # type: ignore
                            else:
                                await self.client.rpush(task.queue.name, content)  # type: ignore
                    except Exception as e:
                        logger.exception(e)
            if count < fetch_count:
                await asyncio.sleep(time_offset)

    async def _receive_message(self, queue_names: tp.Set[str]):
        self._receiving = True
        try:
            while self.running:
                content = await self.client.blpop(
                    list(queue_names), timeout=self.timeout
                )
                if not content:
                    continue
                else:
                    await self.on_message_receive(content[1])
        finally:
            self._receiving = False

    async def _receive_pubsub_message(self, queue_names: tp.Set[str]):
        try:
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
        finally:
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
        asyncio.ensure_future(self._schedule_delayed_message())
