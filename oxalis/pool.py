import asyncio
import logging
import typing as tp
from asyncio.queues import Queue, QueueEmpty

logger = logging.getLogger("oxalis_pool")


class Pool:
    def __init__(
        self,
        limit: int = 0,
        concurrency: int = 100,
        timeout: tp.Union[int, float] = 5 * 60,
    ):
        self.concurrency = limit or concurrency
        self.timeout = timeout
        self.pending_queue: Queue[tp.Tuple[tp.Awaitable, float]] = Queue()
        self.done_queue: Queue = Queue()
        self.futures: tp.Set[asyncio.Future] = set()
        self.running_count = 0
        self.running = True

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} <running_count: {self.running_count}, pending_count: {self.pending_queue.qsize()}>"

    def spawn(
        self, coroutine: tp.Awaitable, pending: bool = True, timeout: float = -1
    ) -> tp.Optional[asyncio.Future]:
        if not self.running:
            raise RuntimeError("This pool has been closed")

        if self.concurrency == -1 or self.running_count < self.concurrency:
            return self._ensure_future(coroutine, timeout=timeout)
        elif pending:
            self.pending_queue.put_nowait((coroutine, timeout))
            return None
        else:
            return None

    def ensure_future(
        self, coroutine: tp.Awaitable, pending: bool = True, timeout: float = -1
    ) -> tp.Optional[asyncio.Future]:
        return self.spawn(coroutine, pending=pending, timeout=timeout)

    def _ensure_future(self, coroutine: tp.Awaitable, timeout: float):
        timeout = self.timeout if timeout == -1 else timeout
        self.running_count += 1
        f = asyncio.ensure_future(asyncio.wait_for(coroutine, timeout=timeout))
        f.add_done_callback(self.on_future_done)
        self.futures.add(f)
        return f

    async def wait_spawn(
        self, coroutine: tp.Awaitable, timeout: float = -1
    ) -> tp.Optional[asyncio.Future]:
        while self.running:
            f = self.spawn(coroutine, pending=False, timeout=timeout)
            if f:
                return f
            else:
                await self.done_queue.get()
        return None

    @property
    def done(self) -> bool:
        return not (self.running_count or self.pending_queue.qsize())

    async def wait_done(self):
        while not self.done:
            await self.done_queue.get()

    def close(self, force=False):
        logger.info(f"Close {'(force)' if force else ''} {self}...")
        self.running = False
        if force:
            self.running = False
            while not self.pending_queue.empty():
                self.pending_queue.get_nowait()
            for f in self.futures:
                f.cancel()

    async def wait_close(self):
        self.close()
        await self.wait_done()

    def force_close(self):
        self.close(force=True)

    def check_future(self, f: asyncio.Future):
        e = f.exception()
        if e:
            try:
                raise e
            except Exception:
                logger.exception(e)

    def on_future_done(self, f: asyncio.Future):
        self.running_count -= 1
        self.done_queue.put_nowait(f)
        self.check_future(f)
        self.futures.remove(f)
        try:
            if self.concurrency == -1 or self.running_count < self.concurrency:
                next, timeout = self.pending_queue.get_nowait()
                self._ensure_future(next, timeout)
        except QueueEmpty:
            pass
