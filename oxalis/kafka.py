from __future__ import annotations

import asyncio
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
        batch_count: int = 1,
        **consumer_kwargs: tp.Any,
    ) -> None:
        self.name = name
        self.consumer_count = consumer_count
        self.batch_count = batch_count
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

    async def _consume(self, message: aiokafka.ConsumerRecord):
        await self.on_message_receive(message.value)
        return message

    async def _batch_consume(
        self,
        topic: Topic,
        consumer: aiokafka.AIOKafkaConsumer,
        messages: tp.List[aiokafka.ConsumerRecord],
    ):
        offsets = sorted([msg.offset for msg in messages])
        assert offsets
        consumed_offsets = set()
        committed_offsets: tp.Set[int] = set()
        offset_index = 0
        for cor in asyncio.as_completed(
            [self._consume(message) for message in messages]
        ):
            msg: aiokafka.ConsumerRecord = await cor
            consumed_offsets.add(msg.offset)
            # commit minimum consumed message offset
            if not topic.enable_auto_commit:
                while (
                    offset_index < len(offsets)
                    and offsets[offset_index] in consumed_offsets
                ):
                    offset_index += 1
                offset_index = offset_index - 1
                if offset_index < 0:
                    continue

                offset = offsets[offset_index]
                if offset in committed_offsets:
                    continue
                try:
                    await consumer.commit(
                        {aiokafka.TopicPartition(msg.topic, msg.partition): offset + 1}
                    )
                    committed_offsets.add(offset)
                except Exception as e:
                    logger.warning(
                        f"Topic<{msg.topic}> commit offset<{offset}> failed: {e}"
                    )

    async def _start_consumer(self, topic: Topic):
        self.consuming_count += 1
        consumer = aiokafka.AIOKafkaConsumer(
            topic.name,
            bootstrap_servers=self.kafka_url,
            group_id=self.group,
            enable_auto_commit=topic.enable_auto_commit,
            **topic.consumer_kwargs,
        )
        pool = Pool(concurrency=-1, timeout=None)
        self.consumers.append(consumer)
        self.pools.append(pool)
        consumer_started = False
        try:
            await consumer.start()
            consumer_started = True
            await asyncio.sleep(
                self.timeout
            )  # wait for rebalancing when boost same topic's consumer in same time
            while self.running:
                data = await consumer.getmany(
                    max_records=topic.batch_count, timeout_ms=self.timeout * 100
                )
                if not data:
                    if pool.done and topic.pause:
                        consumer.resume(*consumer.assignment())
                else:
                    cors = [
                        self._batch_consume(topic, consumer, msgs)
                        for _, msgs in data.items()
                    ]
                    if topic.pause:
                        assert pool.done  # in case
                        consumer.pause(*consumer.assignment())
                        for cor in cors:
                            pool.spawn(cor)
                    else:
                        await asyncio.wait([asyncio.ensure_future(cor) for cor in cors])
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
