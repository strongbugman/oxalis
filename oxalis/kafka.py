from __future__ import annotations

import asyncio
import contextlib
import typing as tp

import aiokafka

from .base import Oxalis as _Oxalis
from .base import Task as _Task
from .base import TaskCodec, logger
from .pool import Pool


class Task(_Task):
    def __init__(
        self,
        oxalis: Oxalis,
        func: tp.Callable,
        topic: str,
        name="",
        timeout: float = -1,
        pool: tp.Optional[Pool] = None,
    ) -> None:
        super().__init__(oxalis, func, name, timeout, pool)
        self.topic = topic


class Oxalis(_Oxalis):
    def __init__(
        self,
        kafka_url: str,
        task_cls: tp.Type[Task] = Task,
        task_codec: TaskCodec = TaskCodec(),
        pool: Pool = Pool(limit=-1),
        timeout: float = 5.0,
        worker_num: int = 0,
        test: bool = False,
        group="default_group",
        default_topic="default_topic",
        topics: tp.Sequence[str] = tuple(),
    ) -> None:
        super().__init__(
            task_cls=task_cls,
            task_codec=task_codec,
            pool=pool,
            timeout=timeout,
            worker_num=worker_num,
            test=test,
        )
        self.pool_wait_spawn = True
        self.consuming = True
        self.tasks: tp.Dict[str, Task] = {}  # type: ignore
        self.kafka_url = kafka_url
        self.default_topic = default_topic
        self.group = group
        self.topics = set(topics)
        self.topics.add(self.default_topic)
        self.producer: aiokafka.AIOKafkaConsumer

    async def connect(self):
        self.producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.kafka_url,
            request_timeout_ms=int(self.timeout * 1000),
        )
        await self.producer.start()

    async def wait_close(self):
        while self.consuming:
            await asyncio.sleep(self.timeout)

    async def disconnect(self):
        await self.producer.stop()

    async def send_task(self, task: Task, *task_args, _delay=0, **task_kwargs):  # type: ignore[override]
        if task.name not in self.tasks:
            raise ValueError(f"Task {task} not register")
        logger.debug(f"Send task {task} to worker...")
        await self.producer.send_and_wait(
            task.topic, self.task_codec.encode(task, task_args, task_kwargs)
        )

    def register(
        self,
        *,
        task_name: str = "",
        timeout: float = -1,
        topic: str = "",
        pool: tp.Optional[Pool] = None,
        **_,
    ) -> tp.Callable[[tp.Callable], Task]:
        if not topic:
            topic = self.default_topic

        def wrapped(func):
            task = self.task_cls(
                self, func, name=task_name, timeout=timeout, topic=topic, pool=pool
            )
            self.register_task(task)
            self.topics.add(topic)
            return task

        return wrapped

    async def _start_consumer(self):
        self.consuming = True
        try:
            consumer = aiokafka.AIOKafkaConsumer(
                *self.topics, bootstrap_servers=self.kafka_url, group_id=self.group
            )
            await consumer.start()
            while self.running:
                with contextlib.suppress(asyncio.TimeoutError):
                    msg = await asyncio.wait_for(
                        consumer.getone(), timeout=self.timeout
                    )
                    await self.on_message_receive(msg.value)
            await consumer.stop()
        finally:
            self.consuming = False

    def _run_worker(self):
        asyncio.ensure_future(self._start_consumer())
