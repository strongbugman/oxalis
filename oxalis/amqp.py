import typing as tp
import asyncio

from aio_pika import connect_robust, Connection, ExchangeType, Queue as _Queue, Exchange as _Exchange, Message, exceptions, abc 
from aiormq.abc import AbstractChannel

from .base import App as _App, TaskCodec, Task, logger
from .pool import Pool



class Exchange(_Exchange):
    def __init__(self, name: str, type: tp.Union[ExchangeType, str] = ExchangeType.DIRECT, *, auto_delete: bool = False, durable: bool = False, internal: bool = False, passive: bool = False, arguments: abc.Arguments = None):
        super().__init__(None, name, type, auto_delete=auto_delete, durable=durable, internal=internal, passive=passive, arguments=arguments)

    def set_channel(self, channel: AbstractChannel):
        self.channel = channel


class Queue(_Queue):
    def __init__(self, name: tp.Optional[str], durable: bool, exclusive: bool, auto_delete: bool, arguments: abc.Arguments, passive: bool = False):
        super().__init__(None, name, durable, exclusive, auto_delete, arguments, passive)
    
    def set_channel(self, channel: AbstractChannel):
        self.channel = channel


class App(_App):
    connection: Connection
    publish_channel: abc.AbstractChannel
    default_exchange: abc.AbstractExchange
    default_queue: abc.AbstractQueue

    def __init__(
        self, url: str, task_codec: TaskCodec = TaskCodec(), pool: Pool = Pool()
    ) -> None:
        super().__init__(task_codec=task_codec, pool=pool)
        self.url = url
        self.exchanges: tp.Dict[str, Exchange] = {}
        self.default_exchange = Exchange("oxalis_default_exchange")
        self.default_queue = Queue("oxalis_default_queue", durable=True, exclusive=False, auto_delete=False, arguments=None, passive=False)
        self.queues: tp.Dict[str, Queue] = {}
        self.routing_keys: tp.Dict[str, str] = {}
        self.default_routing_key = "oxalis_default"
    
    async def connect(self):
        self.connection = await connect_robust(self.url)
        self.publish_channel = self.connection.channel()
        await self.publish_channel.initialize()
        self.default_queue.set_channel(self.publish_channel.channel)
        self.default_exchange.set_channel(self.publish_channel.channel)
        await self.default_queue.declare()
        await self.default_exchange.declare()
        await self.default_queue.bind(self.default_exchange, self.default_routing_key)
    
    async def disconnect(self):
        await self.connection.close()
    
    async def send_task(self, task: Task, *task_args, **task_kwargs):
        if task.name not in self.tasks:
            raise ValueError(f"Task {task} not register")
        logger.debug(f"Send task {task} to worker...")
        exchange = self.exchanges[task.name]
        routing_key = self.routing_keys[task.name]
        if not self.publish_channel.is_initialized:
            await self.publish_channel.initialize()
        exchange.channel = self.publish_channel.channel
        self.publish_channel.declare_exchange
        await exchange.publish(
            Message(
                self.task_codec.encode(task, task_args, task_kwargs),
                content_type='text/plain',
            ),
            routing_key=routing_key
        )

    def register(self, task_name: str = "", queue: tp.Optional[abc.AbstractQueue] = None, exchange: tp.Optional[abc.AbstractExchange] = None, routing_key: str = "", **kwargs) -> tp.Callable[[tp.Callable], Task]:
        def wrapped(func):
            task = Task(func, name=task_name)
            if task.name in self.tasks:
                raise ValueError("double task, check task name")
            self.tasks[task.name] = task
            self.exchanges[task.name] = exchange or self.default_exchange
            self.routing_keys[task.name] = routing_key or self.default_routing_key
            self.queues[task.name] = queue or self.default_queue
            return task

        return wrapped
    
    async def _receive_message(self, queue: Queue):
        async with self.connection.channel() as channel:
            while self.running:
                queue.channel = channel.channel
                try:
                    message = await queue.get(timeout=10)
                    if message:
                        await message.ack()
                        await self.on_message_receive(message.body)
                except exceptions.QueueEmpty:
                    pass

    def _run_worker(self):
        queues = []
        _queues = set()
        for q in self.queues.values():
            if q.name in _queues:
                continue
            else:
                _queues.add(q.name)
                queues.append(q)
            
        for q in queues:
            asyncio.ensure_future(self._receive_message(q))
