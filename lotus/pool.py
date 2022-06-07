import typing as tp
import asyncio
from asyncio.queues import Queue, QueueEmpty
import logging
import async_timeout

logger = logging.getLogger("lotus")


class Pool:
    def __init__(
        self,
        limit: int = 100,
        timeout: tp.Union[int, float] = 5 * 60
    ):
        self.limit = limit
        self.timeout = timeout
        self.pending_queue: Queue = Queue()
        self.done_queue: Queue = Queue()
        self.futures: tp.Set[asyncio.Future] = set()
        self.running_count = 0
        self.running = True

    async def spawn(self, coroutine: tp.Awaitable, block: bool = False):
        if not self.running:
            raise RuntimeError("This pool has been closed")

        while True:
            if self.limit == -1 or self.running_count < self.limit:
                self.running_count += 1
                f = asyncio.ensure_future(self.run_coroutine(coroutine))
                f.add_done_callback(self.on_future_done)
                self.futures.add(f)
                break
            elif not block:
                self.pending_queue.put_nowait(coroutine)
                break
            else:
                await self.done_queue.get()
    
    async def run_coroutine(self, coroutine: tp.Awaitable):
        async with async_timeout.timeout(self.timeout):
            await coroutine

    @property
    def done(self) -> bool:
        return not (
            self.running_count or self.done_queue.qsize() or self.pending_queue.qsize()
        )

    async def wait_done(self):
        while not self.done:
            await self.done_queue.get()

    async def close(self, force=False):
        # TODO: 并发冲突
        self.running = False
        await self.wait_done()
    
    def check_future(self, f: asyncio.Future):
        e = f.exception()
        if e:
            try:
                raise e
            except:
                logger.exception(e)

    def on_future_done(self, f: asyncio.Future):
        self.running_count -= 1
        self.done_queue.put_nowait(f)
        self.check_future(f)
        self.futures.remove(f)
        try:
            if self.running and (self.limit == -1 or self.running_count < self.limit):
                next = self.pending_queue.get_nowait()
                self.running_count += 1
                asyncio.ensure_future(next).add_done_callback(self.on_future_done)
        except QueueEmpty:
            pass
