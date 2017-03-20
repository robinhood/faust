import asyncio
from typing import (
    Any, AsyncIterable, Awaitable, Callable,
    Coroutine, Generator, Iterable, Optional,
)
from ..types import V


class InputStream(Iterable, AsyncIterable):
    queue: asyncio.Queue

    def __init__(self, *, loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop
        self.queue = asyncio.Queue(maxsize=1, loop=self.loop)

    async def put(self, value: V) -> None:
        await self.queue.put(value)

    async def next(self) -> Any:
        # There is no ``anext()`` like there is ``next`` so we provide
        # this convenience method for generators that want to take
        # multiple events at once.
        # Example:
        #   @stream(Topic('foo'))
        #   def s(it):
        #       while 1:
        #           event1 = await it.next()
        #           event2 = await it.next()
        #           yield event1 + event2
        return await self.queue.get()

    async def join(self, timeout=None):
        await asyncio.wait_for(self._join(), timeout, loop=self.loop)

    async def _join(self, interval=0.1):
        while self.queue.qsize():
            await asyncio.sleep(interval)

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> Any:
        return self.queue._get()

    async def __aiter__(self) -> 'AsyncIterable':
        return self

    async def __anext__(self) -> Awaitable:
        return await self.queue.get()


class CoroCallback:
    inbox: InputStream

    def __init__(self, inbox: InputStream,
                 *,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.inbox = inbox
        self.loop = loop

    async def send(self, value: V, outbox: Optional[asyncio.Queue]) -> None:
        await self.inbox.put(value)
        asyncio.ensure_future(self.drain(outbox), loop=self.loop)

    async def join(self) -> None:
        # make sure everything in inqueue is processed.
        await self.inbox.join()

    async def drain(self, outbox: Optional[asyncio.Queue]) -> None:
        new_value = await self._drain()
        if outbox is not None:
            await outbox.put(new_value)

    async def _drain(self):
        raise NotImplementedError()


class GeneratorCoroCallback(CoroCallback):
    gen: Generator

    def __init__(self, gen: Generator, inbox: InputStream,
                 **kwargs) -> None:
        self.gen = gen
        super().__init__(inbox, **kwargs)

    async def _drain(self):
        return self.gen.__next__()


class AsyncCoroCallback(CoroCallback):
    gen: AsyncIterable

    def __init__(self, gen: AsyncIterable, inbox: InputStream,
                 **kwargs) -> None:
        self.gen = gen
        super().__init__(inbox, **kwargs)

    async def _drain(self):
        return await self.gen.__anext__()


class AsyncGeneratorCoroCallback(CoroCallback):
    coro: Coroutine
    gen: AsyncIterable
    gen_started = False

    def __init__(self, coro: Coroutine, inbox: InputStream,
                 **kwargs) -> None:
        self.coro = coro
        self.gen = None
        super().__init__(inbox, **kwargs)

    async def _drain(self):
        if not self.gen_started:
            self.gen = await self.coro
        return await self.gen.__anext__()


def wrap_callback(
        fun: Callable,
        *,
        loop: asyncio.AbstractEventLoop = None) -> CoroCallback:
    loop = loop or asyncio.get_event_loop()
    inbox = InputStream(loop=loop)
    gen = fun(inbox)
    if isinstance(gen, Coroutine):
        return AsyncGeneratorCoroCallback(gen, inbox)
    elif isinstance(gen, AsyncIterable):
        return AsyncCoroCallback(gen, inbox)
    return GeneratorCoroCallback(gen, inbox)
