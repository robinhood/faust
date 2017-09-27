"""Coroutine utilities."""
import asyncio
from typing import Any, AsyncIterator, Awaitable, Coroutine, Generator
from mode import Service
from ..types._coroutines import (
    CoroCallbackT, InputStreamT, StreamCoroutine, StreamCoroutineCallback,
)

__all__ = [
    'StreamCoroutine', 'StreamCoroutineCallback',
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

    async def put(self, value: Any) -> None:
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

    async def _join(self, interval: float = 1):
        while self.queue.qsize():
            await asyncio.sleep(interval, loop=self.loop)

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> Any:
        return self.queue._get()

    def __aiter__(self) -> 'InputStream':
        return self

    async def __anext__(self) -> Awaitable:
        return await self.queue.get()


class CoroCallback(Service, CoroCallbackT):
    inbox: InputStreamT

    def __init__(self,
                 inbox: InputStreamT,
                 callback: StreamCoroutineCallback = None,
                 **kwargs: Any) -> None:
        self.inbox = inbox
        self.callback = callback
        super().__init__(**kwargs)

    async def send(self, value: Any) -> None:
        await self.inbox.put(value)

    async def on_stop(self) -> None:
        await self.inbox.join()

    @Service.task
    async def _drainer(self) -> None:
        drain = self._drain
        callback = self.callback
        while not self.should_stop:
            new_value = await drain()
            await callback(new_value)

    async def _drain(self) -> Any:
        raise NotImplementedError()


class GeneratorCoroCallback(CoroCallback):
    gen: Generator[Any, None, None]

    def __init__(self,
                 gen: Generator[Any, None, None],
                 inbox: InputStreamT,
                 callback: StreamCoroutineCallback = None,
                 **kwargs: Any) -> None:
        self.gen = gen
        super().__init__(inbox, callback, **kwargs)

    async def _drain(self) -> Any:
        return self.gen.__next__()


class AsyncCoroCallback(CoroCallback):
    gen: AsyncIterator[Any]

    def __init__(self,
                 gen: AsyncIterator[Any],
                 inbox: InputStreamT,
                 callback: StreamCoroutineCallback = None,
                 **kwargs: Any) -> None:
        self.gen = gen
        super().__init__(inbox, callback, **kwargs)

    async def _drain(self) -> Any:
        return await self.gen.__anext__()


class AsyncGeneratorCoroCallback(CoroCallback):
    coro: Coroutine[Any, None, None]
    gen: AsyncIterator[Any]
    gen_started = False

    def __init__(self,
                 coro: Coroutine[Any, None, None],
                 inbox: InputStreamT,
                 callback: StreamCoroutineCallback = None,
                 **kwargs: Any) -> None:
        self.coro = coro
        self.gen = None
        super().__init__(inbox, callback, **kwargs)

    async def _drain(self) -> Any:
        if not self.gen_started:
            self.gen_started = True
            self.gen = await self.coro
        return await self.gen.__anext__()


def wrap_callback(
        fun: StreamCoroutine,
        callback: StreamCoroutineCallback = None,
        *,
        loop: asyncio.AbstractEventLoop = None) -> CoroCallbackT:
    loop = loop or asyncio.get_event_loop()
    inbox = InputStream(loop=loop)
    gen = fun(inbox)
    if isinstance(gen, Coroutine):
        return AsyncGeneratorCoroCallback(gen, inbox, callback=callback)
    elif isinstance(gen, AsyncIterator):
        return AsyncCoroCallback(gen, inbox, callback=callback)
    return GeneratorCoroCallback(gen, inbox, callback=callback)
