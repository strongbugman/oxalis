import asyncio
import typing as tp

import aio_pika

from .base import Oxalis as _Oxalis
from .base import Task, TaskCodec, logger
from .pool import Pool

ExchangeType = aio_pika.ExchangeType


class Exchange(aio_pika.Exchange):
    NAME_PREFIX = "oxalis_exchange_"

    def __init__(
        self,
        name: str,
        type: tp.Union[aio_pika.ExchangeType, str] = aio_pika.ExchangeType.DIRECT,
        *,
        auto_delete: bool = False,
        durable: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: aio_pika.abc.Arguments = None,
    ):
        super().__init__(
            None,  # type: ignore
            self.NAME_PREFIX + name,
            type,
            auto_delete=auto_delete,
            durable=durable,
            internal=internal,
            passive=passive,
            arguments=arguments,
        )

    def set_channel(self, channel: aio_pika.abc.AbstractChannel):
        self.channel = channel.channel


class Queue(aio_pika.Queue):
    NAME_PREFIX = "oxalis_queue_"

    def __init__(
        self,
        name: tp.Optional[str],
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: tp.Optional[aio_pika.abc.Arguments] = None,
        passive: bool = False,
    ):
        super().__init__(
            None, self.NAME_PREFIX + name, durable, exclusive, auto_delete, arguments, passive  # type: ignore
        )

    def set_channel(self, channel: aio_pika.abc.AbstractChannel):
        self.channel = channel.channel


class Oxalis(_Oxalis):
    connection: aio_pika.Connection
    channel: aio_pika.abc.AbstractChannel

    def __init__(
        self,
        url: str,
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
            task_codec=task_codec,
            pool=pool,
            timeout=timeout,
            worker_num=worker_num,
            test=test,
        )
        self.url = url
        self.ack_later_tasks: tp.Set[str] = set()
        self.default_exchange = Exchange(default_exchange_name)
        self.default_queue = Queue(
            default_queue_name,
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments=None,
            passive=False,
        )
        self.default_routing_key = default_routing_key
        self.queues: tp.List[Queue] = [self.default_queue]
        self.exchanges: tp.Dict[str, Exchange] = {}
        self.bindings: tp.List[tp.Tuple[Queue, Exchange, str]] = [
            (self.default_queue, self.default_exchange, self.default_routing_key)
        ]
        self.routing_keys: tp.Dict[str, str] = {}

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url, timeout=self.timeout)
        self.channel = self.connection.channel()
        await self.channel.initialize(timeout=self.timeout)
        await self.declare(self.queues)
        await self.declare(self.exchanges.values())
        for q, e, k in self.bindings:
            await self.bind(q, e, k)

    async def disconnect(self):
        await self.connection.close()

    async def send_task(self, task: Task, *task_args, **task_kwargs):
        if task.name not in self.tasks:
            raise ValueError(f"Task {task} not register")
        logger.debug(f"Send task {task} to worker...")
        exchange = self.exchanges[task.name]
        routing_key = self.routing_keys[task.name]
        exchange.set_channel(self.channel)
        await exchange.publish(
            aio_pika.Message(
                self.task_codec.encode(task, task_args, task_kwargs),
                content_type="text/plain",
            ),
            routing_key=routing_key,
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
            task = Task(self, func, name=task_name, timeout=timeout)
            if task.name in self.tasks:
                raise ValueError("double task, check task name")
            self.tasks[task.name] = task
            self.exchanges[task.name] = exchange or self.default_exchange
            self.routing_keys[task.name] = routing_key or self.default_routing_key
            if ack_later:
                self.ack_later_tasks.add(task.name)
            return task

        return wrapped

    def register_queues(self, queues: tp.Sequence[Queue]):
        self.queues.extend(queues)

    def register_binding(self, queue: Queue, exchange: Exchange, routing_key: str = ""):
        self.bindings.append((queue, exchange, routing_key))

    async def declare(self, eqs: tp.Sequence[tp.Union[Queue, Exchange]]):
        for eq in eqs:
            eq.set_channel(self.channel)
            await eq.declare(timeout=self.timeout)

    async def bind(self, queue: Queue, exchange: Exchange, routing_key: str = ""):
        queue.set_channel(self.channel)
        exchange.set_channel(self.channel)
        await queue.bind(exchange, routing_key, timeout=self.timeout)

    async def exec_task(self, task: Task, *task_args, **task_kwargs):
        message = task_args[0]
        task_args = task_args[1:]
        ack_later = task.name in self.ack_later_tasks
        if not ack_later:
            await message.ack()
        await super().exec_task(task, *task_args, **task_kwargs)
        if ack_later:
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
                    pass

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
