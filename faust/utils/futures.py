"""Async I/O Future utilities."""
import asyncio
from collections import deque
from functools import singledispatch
from typing import Any, Awaitable, Deque, Optional

__all__ = ['done_future', 'maybe_async']


def done_future(result: Any = None, *,
                loop: asyncio.AbstractEventLoop = None) -> asyncio.Future:
    """Returns :class:`asyncio.Future` that is already evaluated."""
    f = (loop or asyncio.get_event_loop()).create_future()
    f.set_result(result)
    return f


@singledispatch
async def maybe_async(res: Any) -> Any:
    """Await future if argument is awaitable.

    Examples:
        >>> await maybe_async(regular_function(arg))
        >>> await maybe_async(async_function(arg))
    """
    return res


@maybe_async.register(Awaitable)
async def _(res: Awaitable) -> Any:
    return await res


class PoolSemaphore:
    size: int
    loop: asyncio.AbstractEventLoop

    _val: int
    _paused: bool
    _waiters: Deque[asyncio.Future]
    _notify_released: Optional[asyncio.Future]

    def __init__(self, size: int,
                 *,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.size = size
        self.loop = loop or asyncio.get_event_loop()
        self._val = size
        self._paused = False
        self._waiters = deque()
        self._notify_released = None

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False
        self._wake_up_next()

    async def wait_empty(self) -> None:
        while self._val:
            fut = self._notify_released = asyncio.Future(loop=self.loop)
            await fut

    def locked(self) -> bool:
        return self._val == 0

    def _have_to_wait(self) -> bool:
        return self._paused or self._val <= 0

    async def acquire(self) -> None:
        print("ACQUIRE: PAUSED? %r" % (self._paused,))
        while self._paused or self._val <= 0:
            fut = self.loop.create_future()
            self._waiters.append(fut)
            try:
                await fut
            except:  # noqa
                fut.cancel()
                if self._val > 0 and not fut.cancelled():
                    self._wake_up_next()
                raise
        self._val -= 1

    def release(self) -> None:
        if self._val + 1 > self.size:
            raise ValueError(
                f'{type(self).__name__} released too many times: ')
        self._val += 1
        self._wake_up_next()
        fut = self._notify_released
        if fut is not None and not fut.done():
            fut.set_result(None)

    def _wake_up_next(self) -> None:
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
            break

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self._val - self.size}/{self.size}>'
