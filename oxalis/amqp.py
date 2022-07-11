import typing as tp
import asyncio

import aio_pika

from .base import App as _App, TaskCodec, Task, logger
from .pool import Pool



ExchangeType = aio_pika.ExchangeType


class Exchange(aio_pika.Exchange):
    def __init__(self, name: str, type: tp.Union[aio_pika.ExchangeType, str] = aio_pika.ExchangeType.DIRECT, *, auto_delete: bool = False, durable: bool = False, internal: bool = False, passive: bool = False, arguments: aio_pika.abc.Arguments = None):
        super().__init__(None, name, type, auto_delete=auto_delete, durable=durable, internal=internal, passive=passive, arguments=arguments)

    def set_channel(self, channel: aio_pika.abc.AbstractChannel):
        self.channel = channel.channel


class Queue(aio_pika.Queue):
    def __init__(self, name: tp.Optional[str], durable: bool = False, exclusive: bool = False, auto_delete: bool = False, arguments: tp.Optional[aio_pika.abc.Arguments] = None, passive: bool = False):
        super().__init__(None, name, durable, exclusive, auto_delete, arguments, passive)
    
    def set_channel(self, channel: aio_pika.abc.AbstractChannel):
        self.channel = channel.channel


class App(_App):
    connection: aio_pika.Connection
    channel: aio_pika.abc.AbstractChannel

    def __init__(
        self, url: str, task_codec: TaskCodec = TaskCodec(), pool: Pool = Pool()
    ) -> None:
        super().__init__(task_codec=task_codec, pool=pool)
        self.url = url
        self.ack_later_tasks: tp.Set[str] = set()
        self.default_exchange = Exchange("oxalis_default_exchange")
        self.default_queue = Queue("oxalis_default_queue", durable=True, exclusive=False, auto_delete=False, arguments=None, passive=False)
        self.default_routing_key = "oxalis_default"
        self.queues: tp.List[Queue] = [self.default_queue]
        self.exchanges: tp.Dict[str, Exchange] = {}
        self.bindings: tp.List[tp.Tuple[Queue, Exchange, str]] = [(self.default_queue, self.default_exchange, self.default_routing_key)]
        self.routing_keys: tp.Dict[str, str] = {}
    
    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = self.connection.channel()
        await self.channel.initialize()
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
                content_type='text/plain',
            ),
            routing_key=routing_key
        )

    def register(self, task_name: str = "", exchange: tp.Optional[aio_pika.abc.AbstractExchange] = None, routing_key: str = "", ack_later=False, **_) -> tp.Callable[[tp.Callable], Task]:
        def wrapped(func):
            task = Task(self, func, name=task_name)
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
            await eq.declare()
    
    async def bind(self, queue: Queue, exchange: Exchange, routing_key: str = ""):
        queue.set_channel(self.channel)
        exchange.set_channel(self.channel)
        await queue.bind(exchange, routing_key)
    
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
                queue.channel = channel.channel
                try:
                    message = await queue.get(timeout=10)
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
