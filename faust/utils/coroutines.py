import asyncio
from typing import Any, AsyncIterable, Awaitable, Callable, Optional
from ..types import V


class AsyncInputStream(AsyncIterable):

    queue: asyncio.Queue

    def __init__(self, *,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self.queue = asyncio.Queue(maxsize=1, loop=self.loop)

    async def next(self) -> Any:
        return await self.queue.get()

    async def put(self, value: V) -> None:
        await self.queue.put(value)

    async def __aiter__(self) -> 'AsyncIterable':
        return self

    async def __anext__(self) -> Awaitable:
        return await self.queue.get()

    async def join(self, timeout=None):
        await asyncio.wait_for(self._join(), timeout, loop=self.loop)

    async def _join(self, interval=0.1):
        while self.queue.qsize():
            await asyncio.sleep(interval)


class GeneratorCallback:
    inbox: AsyncInputStream

    def __init__(self, coro: Callable[[AsyncIterable], AsyncIterable],
                 *, loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop
        self.inbox = AsyncInputStream(loop=self.loop)
        self.coro = coro
        self.gen = self.coro(self.inbox)

    async def send(self, value: V, outbox: Optional[asyncio.Queue]) -> None:
        await self.inbox.put(value)
        asyncio.ensure_future(self._drain_gen(outbox), loop=self.loop)

    async def _drain_gen(self, outbox: Optional[asyncio.Queue]) -> None:
        new_value = await self.gen.__anext__()
        if outbox is not None:
            await outbox.put(new_value)

    async def join(self) -> None:
        # make sure everything in inqueue is processed.
        await self.inbox.join()
