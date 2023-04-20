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
        default_routing_key: str = "",
        *,
        auto_delete: bool = False,
        durable: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: aio_pika.abc.Arguments = None,
    ):
        self.type = self._type = type.value if isinstance(type, ExchangeType) else type
        self.name = self.NAME_PREFIX + name
        self.auto_delete = auto_delete
        self.durable = durable
        self.internal = internal
        self.passive = passive
        self.arguments = arguments or {}
        self.default_routing_key = default_routing_key

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
        consumer_prefetch_count: int = 4,
        consumer_prefetch_size: int = 0,
    ):
        self.__get_lock = asyncio.Lock()
        self.close_callbacks = aio_pika.tools.CallbackCollection(self)
        self.name = self.NAME_PREFIX + name
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments
        self.passive = passive
        self.consumer_prefetch_count = consumer_prefetch_count
        self.consumer_prefetch_size = consumer_prefetch_size

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
        ack_later: bool = True,
        ack_always: bool = False,
        reject: bool = True,
        reject_requeue: bool = False,
    ) -> None:
        super().__init__(oxalis, func, name, timeout)
        self.exchange = exchange
        self.routing_key = routing_key
        self.ack_later = ack_later
        self.ack_always = ack_always
        self.reject = reject
        self.reject_requeue = reject_requeue
        if self.ack_always and self.reject:
            raise ValueError("'ack_always=True' conflict with 'reject=True'")
        if not self.ack_later and self.reject:
            raise ValueError("'reject=True' must get alone with 'ack_later=True'")
        if self.ack_always and not self.ack_later:
            raise ValueError("'ack_always=True' must get alone with 'ack_later=True'")
        if self.reject_requeue and not self.reject:
            raise ValueError("'reject_queue=True' need 'reject=True'")


class Oxalis(_Oxalis):
    def __init__(
        self,
        connection: aio_pika.Connection,
        task_cls: tp.Type[Task] = Task,
        task_codec: TaskCodec = TaskCodec(),
        pool: Pool = Pool(limit=-1),
        timeout: float = 5.0,
        worker_num: int = 0,
        test: bool = False,
        default_exchange=Exchange("default", default_routing_key="default"),
        default_queue=Queue("default"),
        default_routing_key="",
    ) -> None:
        super().__init__(
            task_cls=task_cls,
            task_codec=task_codec,
            pool=pool,
            timeout=timeout,
            worker_num=worker_num,
            test=test,
        )
        self.tasks: tp.Dict[str, Task] = {}  # type: ignore
        self.connection = connection
        self.default_exchange = default_exchange
        self.default_queue = default_queue
        if default_routing_key:
            self.default_exchange.default_routing_key = default_routing_key
        self.queues: tp.List[Queue] = [self.default_queue]
        self.exchanges: tp.List[Exchange] = [self.default_exchange]
        self.bindings: tp.List[tp.Tuple[Queue, Exchange, str]] = [
            (
                self.default_queue,
                self.default_exchange,
                self.default_exchange.default_routing_key,
            )
        ]
        self.routing_keys: tp.Dict[str, str] = {}
        self.channels: tp.List[aio_pika.abc.AbstractChannel] = []
        self.consumer_tags: tp.Dict[aio_pika.queue.ConsumerTag, Queue] = {}
        self.pool_wait_spawn = False

    @property
    def channel(self) -> aio_pika.abc.AbstractChannel:
        if not self.channels:
            raise RuntimeError("Call connect first!")
        return self.channels[0]

    async def connect(self):
        self.connection = self.connection.__class__(
            self.connection.url, **self.connection.kwargs
        )
        await self.connection.connect(timeout=self.timeout)
        channel = self.connection.channel()
        await channel.initialize(timeout=self.timeout)
        self.channels.append(channel)
        await self.declare(self.queues)
        await self.declare(self.exchanges)
        for q, e, k in self.bindings:
            await self.bind(q, e, k)

    async def wait_close(self):
        for tag, queue in self.consumer_tags.items():
            await queue.cancel(tag, timeout=self.timeout)
        await asyncio.sleep(self.timeout)  # waiting for reject message

    async def disconnect(self):
        for channel in self.channels:
            await channel.close()
        await self.connection.close()

    async def send_task(self, task: Task, *task_args, _delay: float = 0, _priority: int = 0, _headers: tp.Optional[tp.Dict] = None, **task_kwargs):  # type: ignore[override]
        if task.name not in self.tasks:
            raise ValueError(f"Task {task} not register")
        headers = _headers if _headers else {}
        if _delay:
            if task.exchange.type != ExchangeType.X_DELAYED_MESSAGE:
                raise ValueError(
                    f"Task {task} with delay must go with 'x-delayed-message' exchange"
                )
            headers["x-delay"] = int(_delay * 1000)
            logger.debug(f"Send task {task} to worker with {_delay}s delay...")
        else:
            logger.debug(f"Send task {task} to worker...")
        task.exchange.set_channel(self.channel)
        await task.exchange.publish(
            aio_pika.Message(
                self.task_codec.encode(task, task_args, task_kwargs),
                content_type="text/plain",
                headers=headers,
                priority=_priority,
            ),
            routing_key=task.routing_key,
            timeout=self.timeout,
        )

    def register(
        self,
        *,
        task_name: str = "",
        timeout: float = -1,
        exchange: tp.Optional[Exchange] = None,
        routing_key: str = "",
        ack_later: bool = True,
        ack_always: bool = False,
        reject: bool = True,
        reject_requeue: bool = False,
        **_,
    ) -> tp.Callable[[tp.Callable], Task]:
        if not exchange:
            exchange = self.default_exchange
        if not routing_key:
            assert exchange
            routing_key = exchange.default_routing_key

        def wrapped(func):
            task = self.task_cls(
                self,
                func,
                exchange,
                routing_key,
                name=task_name,
                timeout=timeout,
                ack_later=ack_later,
                ack_always=ack_always,
                reject=reject,
                reject_requeue=reject_requeue,
            )
            self.register_task(task)
            self.exchanges.append(task.exchange)
            return task

        return wrapped

    def on_worker_init(self):
        super().on_worker_init()
        self.channels.clear()

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
        try:
            await super().exec_task(task, *task_args, **task_kwargs)
        except Exception as e:
            if task.reject:
                await message.reject(requeue=task.reject_requeue)
            elif task.ack_always and task.ack_later:
                await message.ack()
            raise e from None
        if task.ack_later:
            await message.ack()

    async def _on_message_receive(self, message: aio_pika.abc.AbstractIncomingMessage):
        task, spawned = await self.on_message_receive(message.body, message)
        # reject after close
        if not task:
            await message.reject()
        elif task and not spawned:
            await message.reject(requeue=True)

    async def _receive_message(self, queue: Queue):
        channel = self.connection.channel()
        self.channels.append(channel)
        await channel.initialize()
        await channel.set_qos(
            prefetch_count=queue.consumer_prefetch_count,
            prefetch_size=queue.consumer_prefetch_size,
            global_=True,
        )
        queue.set_channel(channel)
        tag = await queue.consume(self._on_message_receive)
        self.consumer_tags[tag] = queue

    def _run_worker(self):
        """
        Limit queue consume concurrency by AMQP's QOS config
        """
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
