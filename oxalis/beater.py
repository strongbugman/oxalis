import asyncio
import signal
import time
import typing as tp

from croniter import croniter

from .base import Oxalis, Task, logger


class Beater:
    def __init__(self, oxalis: Oxalis) -> None:
        self.oxalis = oxalis
        self.tasks: tp.List[Task] = []
        self.croniteres: tp.List[croniter] = []
        self.crons: tp.List[str] = []
        self.futures: tp.List[asyncio.Future] = []
        self.running = False

    def register(self, cron: str, task: Task):
        self.tasks.append(task)
        self.crons.append(cron)
        self.croniteres.append(croniter(cron))

    async def beat(self, i: int):
        t = self.croniteres[i].get_next() - time.time()
        await asyncio.sleep(t)
        await self.tasks[i].delay()
        logger.info(f"Beat task {self.tasks[i]}")
        self.futures[i] = asyncio.ensure_future(self.beat(i))

    async def _run(self):
        while self.running:
            await asyncio.sleep(0.5)

    def close(self, *_):
        logger.info("Close beater...")
        self.running = False
        for f in self.futures:
            f.cancel()

    def run(self):
        self.running = True
        signal.signal(signal.SIGINT, self.close)
        signal.signal(signal.SIGTERM, self.close)
        asyncio.get_event_loop().run_until_complete(self.oxalis.connect())
        for i in range(len(self.tasks)):
            logger.info(f"Beat task: {self.tasks[i]} at <{self.crons[i]}> ...")
            self.futures.append(asyncio.ensure_future(self.beat(i)))
        asyncio.get_event_loop().run_until_complete(self._run())
        asyncio.get_event_loop().run_until_complete(self.oxalis.disconnect())
