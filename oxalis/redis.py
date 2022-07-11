import typing as tp
import asyncio

from aioredis.client import Redis

from .base import App as _App, TaskCodec, Task, logger
from .pool import Pool


class Queue:
    NAME_PREFIX = "oxalis_queue_"

    def __init__(self, name: str) -> None:
        self.name = self.NAME_PREFIX + name


class PubsubQueue(Queue):
    pass



class App(_App):
    def __init__(
        self, client: Redis, task_codec: TaskCodec = TaskCodec(), pool: Pool = Pool()
    ) -> None:
        super().__init__(task_codec=task_codec, pool=pool)
        self.client = client
        self.pubsub = client.pubsub()
        self.queues: tp.Dict[str, Queue] = {}
        self.default_queue = Queue("default")
    
    async def connect(self):
        await self.client.initialize()
    
    async def disconnect(self):
        await self.client.close()
    
    async def send_task(self, task: Task, *task_args, **task_kwargs):
        if task.name not in self.tasks:
            raise ValueError(f"Task {task} not register")
        logger.debug(f"Send task {task} to worker...")
        queue = self.queues[task.name]
        content = self.task_codec.encode(task, task_args, task_kwargs)
        if isinstance(queue, PubsubQueue):
            await self.client.publish(queue.name, content)
        else:
            await self.client.rpush(queue.name, content)

    def register(self, task_name: str = "", queue: tp.Optional[Queue] = None, **kwargs) -> tp.Callable[[tp.Callable], Task]:
        def wrapped(func):
            task = Task(self, func, name=task_name)
            if task.name in self.tasks:
                raise ValueError("double task, check task name")
            self.tasks[task.name] = task
            self.queues[task.name] = queue or self.default_queue
            return task

        return wrapped

    async def _receive_message(self, timeout: tp.Union[int, float] = 0.5):
        while self.running:
            content = await self.client.blpop(
                list({q.name for q in self.queues.values() if not isinstance(q, PubsubQueue)}), timeout=timeout
            )
            if not content:
                continue
            else:
                await self.on_message_receive(content[1])
    
    async def _receive_pubsub_message(self, timeout: tp.Union[int, float]=0.5):
        while self.running:
            if not self.pubsub.subscribed:
                await self.pubsub.subscribe(*{q.name for q in self.queues.values() if isinstance(q, PubsubQueue)})
            content = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)
            if not content:
                continue
            else:
                await self.on_message_receive(content["data"])

    def _run_worker(self):
        if [q for q in self.queues.values() if isinstance(q, PubsubQueue)]:
            asyncio.ensure_future(self._receive_pubsub_message())
        if [q for q in self.queues.values() if not isinstance(q, PubsubQueue)]:
            asyncio.ensure_future(self._receive_message())
