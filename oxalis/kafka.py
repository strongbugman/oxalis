from __future__ import annotations

import asyncio
import contextlib
import typing as tp

import aiokafka

from .base import PARAM, RT
from .base import Oxalis as _Oxalis
from .base import Task as _Task
from .base import TaskCodec, logger
from .pool import Pool

TASK_TV = tp.TypeVar("TASK_TV", bound="Task")


class Topic:
    def __init__(
        self,
        name: str,
        consumer_count: int = 3,
        pause: bool = False,
        enable_auto_commit: bool = True,
        **consumer_kwargs: tp.Any,
    ) -> None:
        self.name = name
        self.consumer_count = consumer_count
        self.pause = pause
        self.enable_auto_commit = enable_auto_commit
        self.consumer_kwargs = consumer_kwargs

    def __hash__(self) -> int:
        return hash(self.name)


class Task(_Task[PARAM, RT]):
    def __init__(
        self,
        oxalis: Oxalis,
        func: tp.Callable,
        topic: Topic,
        name: str = "",
        timeout: float = -1,
        pool: tp.Optional[Pool] = None,
    ) -> None:
        super().__init__(oxalis, func, name, timeout, pool)
        self.topic = topic
        self.key: bytes | None = None
        self.partition: int | None = None

    def config(
        self: TASK_TV,
        key: bytes | None = None,
        partition: int | None = None,
        **__,
    ) -> TASK_TV:
        self.key = key
        self.partition = partition
        return self

    def clean_config(self) -> None:
        self.key = None
        self.partition = None


class Oxalis(_Oxalis[Task]):
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
        default_topic=Topic("default_topic"),
        topics: tp.Sequence[Topic] = tuple(),
        producer_kwargs: tp.Dict[str, tp.Any] = {},
    ) -> None:
        super().__init__(
            task_cls,
            task_codec=task_codec,
            pool=pool,
            timeout=timeout,
            worker_num=worker_num,
            test=test,
        )
        self.kafka_url = kafka_url
        self.default_topic = default_topic
        self.group = group
        self.topics = set(topics)
        self.topics.add(self.default_topic)
        self.producer_kwargs = producer_kwargs
        self.producer: aiokafka.AIOKafkaProducer
        self.consumers: tp.List[aiokafka.AIOKafkaConsumer] = []

    async def connect(self):
        self.producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.kafka_url,
            request_timeout_ms=int(self.timeout * 1000),
            **self.producer_kwargs,
        )
        await self.producer.start()

    async def disconnect(self):
        await self.producer.stop()

    async def send_task(self, task: Task, *task_args, **task_kwargs):
        if task.name not in self.tasks:
            raise ValueError(f"Task {task} not register")
        logger.debug(f"Send task {task} to worker...")
        await self.producer.send_and_wait(
            task.topic.name,
            value=self.task_codec.encode(task, task_args, task_kwargs),
            key=task.key,
            partition=task.partition,
        )

    def register(
        self,
        *,
        task_name: str = "",
        timeout: float = -1,
        topic: tp.Optional[Topic] = None,
        **_,
    ) -> tp.Callable[
        [tp.Callable[PARAM, tp.Union[tp.Awaitable[RT], RT]]], Task[PARAM, RT]
    ]:
        if not topic:
            topic = self.default_topic

        def wrapped(func):
            task = self.task_cls(
                self,
                func,
                topic,
                name=task_name,
                timeout=timeout,
            )
            self.register_task(task)
            self.topics.add(topic)
            return task

        return wrapped

    async def _start_consumer(self, topic: Topic):
        self.consuming_count += 1
        consumer = aiokafka.AIOKafkaConsumer(
            topic.name,
            bootstrap_servers=self.kafka_url,
            group_id=self.group,
            enable_auto_commit=topic.enable_auto_commit,
            **topic.consumer_kwargs,
        )
        self.consumers.append(consumer)
        consumer_started = False
        try:
            await consumer.start()
            consumer_started = True
            await asyncio.sleep(
                self.timeout
            )  # wait for rebalancing when boost same topic's consumer in same time
            while self.running:
                with contextlib.suppress(asyncio.TimeoutError):
                    msg = await asyncio.wait_for(
                        consumer.getone(), timeout=self.timeout
                    )
                    if topic.pause:
                        consumer.pause(*consumer.assignment())
                    await self.on_message_receive(msg.value)
                    if topic.pause:
                        consumer.resume(*consumer.assignment())
                    if not topic.enable_auto_commit:
                        try:
                            await consumer.commit()
                        except Exception as e:
                            logger.warning(
                                f"Topic<{topic}> Commit <{consumer.assignment()}> failed: {e}"
                            )
        except Exception as e:
            self.health = False
            raise e from None
        finally:
            if consumer_started:
                await asyncio.sleep(self.timeout)  # wait for committing
                await consumer.stop()
            self.consuming_count -= 1

    def _run_worker(self):
        for t in self.topics:
            for _ in range(t.consumer_count):
                asyncio.ensure_future(self._start_consumer(t))
