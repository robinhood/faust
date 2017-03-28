"""Coroutine utilities."""
import asyncio
from typing import Any, AsyncIterable, Awaitable, Coroutine, Generator
from ..types.coroutines import (
    CoroCallbackT, InputStreamT, StreamCoroutineCallback,
)
from ..types.models import Event
from ..types.streams import StreamCoroutine

__all__ = [
    'InputStream', 'CoroCallback',
    'GeneratorCoroCallback', 'AsyncCoroCallback', 'AsyncGeneratorCoroCallback',
    'wrap_callback',
]

# This implements the Stream(coroutine=x) stuff where the stream can be
# processsed by a generator like:
#
#   def filter_large_withdrawals(it):
#       return (e for e in it if e.amount > 999.0)
#
# or an async generator like:
#
#   async def filter_large_withdrawals(it):
#       return (e async for e in it if e.amount > 999.0)
#
# or an async coroutine like:
#
#   async def filter_large_withdrawals(it):
#       while 1:
#         event1 = it.next()
#         event2 = it.next()
#         yield event1.derive(amount=event1.amount + event2.amount)


class InputStream(InputStreamT):

    def __init__(self, *, loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop
        self.queue = asyncio.Queue(maxsize=1, loop=self.loop)

    async def put(self, value: Event) -> None:
        await self.queue.put(value)

    async def next(self) -> Any:
        # There is no ``anext()`` like there is ``next`` so we provide
        # this convenience method for generators that want to take
        # multiple events at once.
        # Example:
        #   def s(it):
        #       while 1:
        #           event1 = await it.next()
        #           event2 = await it.next()
        #           yield event1 + event2
        return await self.queue.get()

    async def join(self, timeout: float = None):
        await asyncio.wait_for(self._join(), timeout, loop=self.loop)

    async def _join(self, interval: float = 0.1):
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


class CoroCallback(CoroCallbackT):
    inbox: InputStreamT

    def __init__(self, inbox: InputStreamT,
                 *,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.inbox = inbox
        self.loop = loop

    async def send(self,
                   value: Event,
                   callback: StreamCoroutineCallback) -> None:
        await self.inbox.put(value)
        asyncio.ensure_future(self.drain(callback), loop=self.loop)

    async def join(self) -> None:
        # make sure everything in inqueue is processed.
        await self.inbox.join()

    async def drain(self, callback: StreamCoroutineCallback) -> None:
        new_value = await self._drain()
        await callback(new_value)

    async def _drain(self):
        raise NotImplementedError()


class GeneratorCoroCallback(CoroCallback):
    gen: Generator[Event, None, None]

    def __init__(self,
                 gen: Generator[Event, None, None],
                 inbox: InputStreamT,
                 **kwargs: Any) -> None:
        self.gen = gen
        super().__init__(inbox, **kwargs)

    async def _drain(self):
        return self.gen.__next__()


class AsyncCoroCallback(CoroCallback):
    gen: AsyncIterable[Event]

    def __init__(self,
                 gen: AsyncIterable[Event],
                 inbox: InputStreamT,
                 **kwargs: Any) -> None:
        self.gen = gen
        super().__init__(inbox, **kwargs)

    async def _drain(self):
        return await self.gen.__anext__()


class AsyncGeneratorCoroCallback(CoroCallback):
    coro: Coroutine[Event, None, None]
    gen: AsyncIterable[Event]
    gen_started = False

    def __init__(self,
                 coro: Coroutine[Event, None, None],
                 inbox: InputStreamT,
                 **kwargs: Any) -> None:
        self.coro = coro
        self.gen = None
        super().__init__(inbox, **kwargs)

    async def _drain(self):
        if not self.gen_started:
            self.gen_started = True
            self.gen = await self.coro
        return await self.gen.__anext__()


def wrap_callback(
        fun: StreamCoroutine,
        *,
        loop: asyncio.AbstractEventLoop = None) -> CoroCallbackT:
    loop = loop or asyncio.get_event_loop()
    inbox = InputStream(loop=loop)
    gen = fun(inbox)
    if isinstance(gen, Coroutine):
        return AsyncGeneratorCoroCallback(gen, inbox)
    elif isinstance(gen, AsyncIterable):
        return AsyncCoroCallback(gen, inbox)
    return GeneratorCoroCallback(gen, inbox)
