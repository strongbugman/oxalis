from __future__ import annotations

import abc
import asyncio
import inspect
import json
import logging
import multiprocessing
import os
import signal
import sys
import typing as tp

from .pool import Pool

logger = logging.getLogger("oxalis")


TASK_TV = tp.TypeVar("TASK_TV", bound="Task")


class Task:
    def __init__(
        self,
        oxalis: Oxalis,
        func: tp.Callable,
        name="",
        timeout: float = -1,
        pool: tp.Optional[Pool] = None,
    ) -> None:
        self.oxalis = oxalis
        self.func = func
        self.name = name or self.get_name()
        self.timeout = timeout
        self.pool = pool or oxalis.pools[0]

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.name})>"

    async def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> tp.Any:
        ret = self.func(*args, **kwargs)
        if inspect.iscoroutine(ret):
            ret = await ret

        return ret

    async def delay(self, *args, _delay: float = 0, **kwargs) -> tp.Any:
        if self.oxalis.test:
            return await self(*args, **kwargs)
        else:
            await self.oxalis.send_task(self, *args, _delay=_delay, **kwargs)

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


class Oxalis(abc.ABC):
    def __init__(
        self,
        task_cls: tp.Type[Task] = Task,
        task_codec: TaskCodec = TaskCodec(),
        pool: Pool = Pool(),
        timeout: float = 5.0,
        worker_num: int = 0,
        test: bool = False,
    ) -> None:
        self.task_cls = task_cls
        self.tasks: tp.Dict[str, Task] = {}
        self.task_codec = task_codec
        self.pools: tp.List[Pool] = [pool]
        self.running = False
        self.timeout = timeout
        self.test = test
        self._on_close_signal_count = 0
        self.worker_num = worker_num or os.cpu_count()
        self.is_worker = False
        self.pool_wait_spawn = True

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(pid-{os.getpid()})>"

    async def connect(self):
        pass

    async def wait_close(self):
        pass

    async def disconnect(self):
        pass

    @abc.abstractmethod
    async def send_task(
        self,
        task: Task,
        *task_args,
        _delay: float = 0,
        **task_kwargs,
    ):
        pass

    async def exec_task(self, task: Task, *task_args, **task_kwargs):
        logger.debug(f"Worker {self} execute task {task}...")
        await task(*task_args, **task_kwargs)

    def run_worker_master(self):
        for task in self.tasks.values():
            logger.info(f"Registered Task: {task}")

        signal.signal(signal.SIGINT, self.close)
        signal.signal(signal.SIGTERM, self.close)
        ps = []
        for _ in range(self.worker_num):
            ps.append(multiprocessing.Process(target=self.run_worker))
            ps[-1].start()
        for p in ps:
            p.join()

    def run_worker(self):
        logger.info(f"Run worker: {self}...")
        self.running = True
        self.is_worker = True
        self.on_worker_init()
        asyncio.get_event_loop().run_until_complete(self.connect())
        self._run_worker()
        asyncio.get_event_loop().run_until_complete(self.work())
        self.on_worker_close()

    @abc.abstractmethod
    def _run_worker(self):
        pass

    async def work(self):
        while self.running:
            await asyncio.sleep(self.timeout)

        await self.wait_close()
        await asyncio.wait(
            [asyncio.get_event_loop().create_task(p.wait_close()) for p in self.pools],
            timeout=self.timeout,
        )
        await self.disconnect()

    def close_worker(self, force: bool = False):
        logger.info(f"Close worker{'(force)' if force else ''}: {self}...")
        self.running = False
        if force:
            logger.warning(f"Force close: {self}, may lose some message!")
            for p in self.pools:
                p.force_close()
            sys.exit()

    def register_task(self, task: Task):
        if task.name in self.tasks:
            raise ValueError("double task, check task name")
        self.tasks[task.name] = task

    def register(
        self,
        *,
        task_name: str = "",
        timeout: float = -1,
        pool: tp.Optional[Pool] = None,
        **_,
    ) -> tp.Callable[[tp.Callable], Task]:
        def wrapped(func):
            task = self.task_cls(self, func, name=task_name, timeout=timeout, pool=pool)
            self.register_task(task)
            return task

        return wrapped

    async def on_message_receive(
        self, content: bytes, *args
    ) -> tp.Tuple[tp.Optional[Task], bool]:
        try:
            task_name, task_args, task_kwargs = self.task_codec.decode(content)
        except Exception as e:
            logger.exception(e)
            return None, False

        if task_name not in self.tasks:
            logger.warning(f"Received task {task_name} not found")
            return None, False
        else:
            task = self.tasks[task_name]
            pool = task.pool
            if self.pool_wait_spawn:
                if await pool.wait_spawn(
                    self.exec_task(task, *args, *task_args, **task_kwargs),
                    timeout=task.timeout,
                ):
                    return task, True
                else:
                    logger.warning(
                        f"Spawn task to closed pool, message {task} may losed"
                    )
                    return task, False
            else:
                if pool.running:
                    pool.spawn(
                        self.exec_task(task, *args, *task_args, **task_kwargs),
                        timeout=task.timeout,
                    )
                    return task, True
                else:
                    logger.warning(
                        f"Spawn task to closed pool, message {task} may losed"
                    )
                    return task, False

    def close(self, *_):
        """Close self but wait pool"""
        if not self.is_worker:
            return
        self._on_close_signal_count += 1
        self.close_worker(force=self._on_close_signal_count >= 2)

    def on_worker_init(self):
        pass

    def on_worker_close(self):
        pass
