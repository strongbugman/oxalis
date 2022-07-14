from __future__ import annotations

import asyncio
import typing as tp

import aio_pika

from .base import Oxalis as _Oxalis
from .base import Task as _Task
from .base import TaskCodec, logger
from .pool import Pool

ExchangeType = aio_pika.ExchangeType


class Exchange(aio_pika.Exchange):
    NAME_PREFIX = "oxalis_exchange_"

    def __init__(
        self,
        name: str,
        type: tp.Union[ExchangeType, str] = ExchangeType.DIRECT,
        *,
        auto_delete: bool = False,
        durable: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: aio_pika.abc.Arguments = None,
    ):
        self._type = type.value if isinstance(type, ExchangeType) else type
        self.name = self.NAME_PREFIX + name
        self.auto_delete = auto_delete
        self.durable = durable
        self.internal = internal
        self.passive = passive
        self.arguments = arguments or {}

    def set_channel(self, channel: aio_pika.abc.AbstractChannel):
        self.channel = channel.channel


class Queue(aio_pika.Queue):
    NAME_PREFIX = "oxalis_queue_"

    def __init__(
        self,
        name: str,
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: tp.Optional[aio_pika.abc.Arguments] = None,
        passive: bool = False,
    ):
        self.__get_lock = asyncio.Lock()
        self.close_callbacks = aio_pika.tools.CallbackCollection(self)
        self.name = self.NAME_PREFIX + name
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments
        self.passive = passive

    def set_channel(self, channel: aio_pika.abc.AbstractChannel):
        self.channel = channel.channel


class Task(_Task):
    def __init__(
        self,
        oxalis: Oxalis,
        func: tp.Callable,
        exchange: Exchange,
        routing_key: str,
        name="",
        timeout: float = -1,
        ack_later: bool = False,
    ) -> None:
        super().__init__(oxalis, func, name, timeout)
        self.exchange = exchange
        self.routing_key = routing_key
        self.ack_later = ack_later


class Oxalis(_Oxalis):
    channel: aio_pika.abc.AbstractChannel

    def __init__(
        self,
        connection: aio_pika.Connection,
        task_cls: tp.Type[Task] = Task,
        task_codec: TaskCodec = TaskCodec(),
        pool: Pool = Pool(),
        timeout: float = 5.0,
        worker_num: int = 0,
        test: bool = False,
        default_queue_name="default",
        default_exchange_name="default",
        default_routing_key="default",
    ) -> None:
        super().__init__(
            task_cls=task_cls,
            task_codec=task_codec,
            pool=pool,
            timeout=timeout,
            worker_num=worker_num,
            test=test,
        )
        self.connection = connection
        self.ack_later_tasks: tp.Set[str] = set()
        self.default_exchange = Exchange(default_exchange_name)
        self.default_queue = Queue(default_queue_name)
        self.default_routing_key = default_routing_key
        self.queues: tp.List[Queue] = [self.default_queue]
        self.exchanges: tp.List[Exchange] = [self.default_exchange]
        self.bindings: tp.List[tp.Tuple[Queue, Exchange, str]] = [
            (self.default_queue, self.default_exchange, self.default_routing_key)
        ]
        self.routing_keys: tp.Dict[str, str] = {}

    async def connect(self):
        await self.connection.connect(timeout=self.timeout)
        self.channel = self.connection.channel()
        await self.channel.initialize(timeout=self.timeout)
        await self.declare(self.queues)
        await self.declare(self.exchanges)
        for q, e, k in self.bindings:
            await self.bind(q, e, k)

    async def disconnect(self):
        await self.connection.close()

    async def send_task(self, task: Task, *task_args, **task_kwargs):  # type: ignore[override]
        if task.name not in self.tasks:
            raise ValueError(f"Task {task} not register")
        logger.debug(f"Send task {task} to worker...")
        task.exchange.set_channel(self.channel)
        await task.exchange.publish(
            aio_pika.Message(
                self.task_codec.encode(task, task_args, task_kwargs),
                content_type="text/plain",
            ),
            routing_key=task.routing_key,
            timeout=self.timeout,
        )

    def register(
        self,
        task_name: str = "",
        timeout: float = -1,
        exchange: tp.Optional[aio_pika.abc.AbstractExchange] = None,
        routing_key: str = "",
        ack_later: bool = False,
        **_,
    ) -> tp.Callable[[tp.Callable], Task]:
        def wrapped(func):
            task = self.task_cls(
                self,
                func,
                exchange or self.default_exchange,
                routing_key or self.default_routing_key,
                name=task_name,
                timeout=timeout,
                ack_later=ack_later,
            )
            self.register_task(task)
            self.exchanges.append(task.exchange)
            return task

        return wrapped

    def register_queues(self, queues: tp.Sequence[Queue]):
        self.queues.extend(queues)

    def register_exchanges(self, exchanges: tp.Sequence[Exchange]):
        self.exchanges.extend(exchanges)

    def register_binding(self, queue: Queue, exchange: Exchange, routing_key: str = ""):
        self.bindings.append((queue, exchange, routing_key))

    async def declare(self, eqs: tp.Sequence[tp.Union[Queue, Exchange]]):
        _names = set()
        for eq in eqs:
            if eq.name in _names:
                continue
            eq.set_channel(self.channel)
            await eq.declare(timeout=self.timeout)
            _names.add(eq.name)

    async def bind(self, queue: Queue, exchange: Exchange, routing_key: str = ""):
        queue.set_channel(self.channel)
        exchange.set_channel(self.channel)
        await queue.bind(exchange, routing_key, timeout=self.timeout)

    async def exec_task(self, task: Task, *args, **task_kwargs):  # type: ignore[override]
        message: aio_pika.IncomingMessage = args[0]
        task_args = args[1:]
        if not task.ack_later:
            await message.ack()
        await super().exec_task(task, *task_args, **task_kwargs)
        if task.ack_later:
            await message.ack()

    async def _receive_message(self, queue: Queue):
        async with self.connection.channel() as channel:
            while self.running:
                queue.set_channel(channel)
                try:
                    message = await queue.get(timeout=self.timeout)
                    if message:
                        await self.on_message_receive(message.body, message)
                except aio_pika.exceptions.QueueEmpty:
                    await asyncio.sleep(0.1)

    def _run_worker(self):
        queues = []
        _queues = set()
        for q in self.queues:
            if q.name in _queues:
                continue
            else:
                _queues.add(q.name)
                queues.append(q)

        for q in queues:
            asyncio.ensure_future(self._receive_message(q))
