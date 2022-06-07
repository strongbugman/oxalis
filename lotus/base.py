from audioop import mul
import functools
import typing as tp
import asyncio
import abc
import inspect
import json
import os
import sys
import multiprocessing
import signal
import logging

from aioredis.client import Redis

logger = logging.getLogger("lotus")

from .pool import Pool


class Task:
    def __init__(self, func: tp.Callable, name="") -> None:
        self.func = func
        self.name = name or self.get_name()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.name})>"

    async def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> tp.Any:
        ret = self.func(*args, **kwargs)
        if inspect.iscoroutine(ret):
            ret = await ret

        return ret

    def get_name(self) -> str:
        return ".".join((self.func.__module__, self.func.__name__))


class TaskCodec:
    MESSAGE_TYPE = tp.Tuple[str, tp.Sequence[tp.Any], tp.Dict[str, tp.Any]]

    @classmethod
    def encode(
        cls,
        task: Task,
        task_args: tp.Sequence[tp.Any],
        task_kwargs: tp.Dict[str, tp.Any],
    ) -> bytes:
        return json.dumps([task.name, list(task_args), task_kwargs]).encode()

    @classmethod
    def decode(cls, content: bytes) -> MESSAGE_TYPE:
        return json.loads(content)


class Queue:
    name_prefix = "lotus_queue_"

    def __init__(self, name: str) -> None:
        self.name = self.name_prefix + name


class FanOutQueue(Queue):
    pass



class Lotus(abc.ABC):
    def __init__(
        self, task_codec: TaskCodec = TaskCodec(), pool: Pool = Pool()
    ) -> None:
        self.task_queue_map: tp.Dict[str, Queue] = {}
        self.tasks: tp.Dict[str, Task] = {}
        self.default_queue = Queue("default")
        self.task_codec = task_codec
        self.pool = pool
        self.running = False
        self.on_init()
        self._on_close_signal_count = 0

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(pid-{os.getpid()})>"

    def register_task(self, task: Task, queue: tp.Optional[Queue] = None):
        if task.name in self.tasks:
            raise ValueError("Double task, check task name")
        self.tasks[task.name] = task
        self.task_queue_map[task.name] = queue or self.default_queue

    async def send_task(self, task: Task, *task_args, **task_kwargs):
        if task.name not in self.task_queue_map:
            raise ValueError(f"Task {task} not register")
        logger.debug(f"Send task {task} to worker...")
        await self.send_message(
            self.task_queue_map[task.name],
            self.task_codec.encode(task, task_args, task_kwargs),
        )

    async def exec_task(self, task: Task, *task_args, **task_kwargs):
        logger.debug(f"Worker {self} execute task {task}...")
        await task(*task_args, **task_kwargs)

    @abc.abstractmethod
    async def send_message(self, queue: Queue, encode_content: bytes):
        pass

    @abc.abstractmethod
    async def receive_message(
        self, timeout: tp.Union[int, float] = 0.5
    ) -> tp.Optional[bytes]:
        pass

    @abc.abstractmethod
    async def receive_fanout_message(
        self, timeout: tp.Union[int, float] = 0.5
    ) -> tp.Optional[bytes]:
        pass

    def run_worker_master(self):
        signal.signal(signal.SIGINT, self.on_signal)
        signal.signal(signal.SIGTERM, self.on_signal)
        ps = []
        for _ in range(4):
            ps.append(multiprocessing.Process(target=self.run_worker))
            ps[-1].start()
        for p in ps:
            p.join()

    def run_worker(self):
        logger.info(f"Run worker: {self}...")
        self.running = True
        self.on_worker_init()
        if [q for q in self.task_queue_map.values() if isinstance(q, FanOutQueue)]:
            asyncio.ensure_future(self._receive_fanout_message())
        if [q for q in self.task_queue_map.values() if not isinstance(q, FanOutQueue)]:
            asyncio.ensure_future(self._receive_message())
        asyncio.get_event_loop().run_until_complete(self._run_worker())

    async def _receive_message(self):
        while self.running:
            await self.on_message_receive(await self.receive_message())
    
    async def _receive_fanout_message(self):
        while self.running:
            await self.on_message_receive(await self.receive_fanout_message())

    async def _run_worker(self):
        while self.running:
            await asyncio.sleep(0.5)
        await self.pool.close()

    def close_worker(self, force: bool = False):
        logger.info(f"Close worker{'(force)' if force else ''}: {self}...")
        self.running = False
        if force:
            sys.exit()

    def register(self, queue: tp.Optional[Queue] = None, task_name: str = "") -> tp.Callable[[tp.Callable], Task]:
        def wrapped(func):
            task = Task(func, name=task_name)
            self.register_task(task, queue=queue)
            return task

        return wrapped
    
    async def on_message_receive(self, content: bytes):
        if not content:
            return
        task_name, task_args, task_kwargs = self.task_codec.decode(content)
        if task_name not in self.tasks:
            raise ValueError(f"Task {task_name} not found")
        await self.pool.spawn(self.exec_task(self.tasks[task_name], *task_args, **task_kwargs), block=True)
    
    def on_signal(self, *args):
        self._on_close_signal_count += 1
        self.close_worker(force=self._on_close_signal_count >= 2)
    
    def on_init(self):
        pass

    def on_worker_init(self):
        pass


class RedisLotus(Lotus):
    def __init__(
        self, client: Redis, task_codec: TaskCodec = TaskCodec(), pool: Pool = Pool()
    ) -> None:
        super().__init__(task_codec=task_codec, pool=pool)
        self.client = client
        self.pubsub = client.pubsub()
    
    async def receive_message(
        self, timeout: tp.Union[int, float] = 0.5
    ) -> tp.Optional[bytes]:
        content = await self.client.blpop(
            [q.name for q in self.task_queue_map.values() if not isinstance(q, FanOutQueue)], timeout=timeout
        )
        if not content:
            return None
        else:
            return content[1]

    async def receive_fanout_message(
        self, timeout: tp.Union[int, float] = 0.5
    ) -> tp.Optional[bytes]:
        if not self.pubsub.subscribed:
            await self.pubsub.subscribe(*[q.name for q in self.task_queue_map.values() if isinstance(q, FanOutQueue)])
        content = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)
        if not content:
            return None
        else:
            return content["data"]

    async def send_message(self, queue: Queue, encode_content: bytes):
        if isinstance(queue, FanOutQueue):
            await self.client.publish(queue.name, encode_content)
        else:
            await self.client.rpush(queue.name, encode_content)
