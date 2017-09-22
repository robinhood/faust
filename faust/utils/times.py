import abc
import asyncio
from datetime import timedelta
from functools import singledispatch
from time import monotonic
from types import TracebackType
from typing import AsyncContextManager, Optional, Type, Union

__all__ = ['Seconds', 'want_seconds']

#: Seconds can be expressed as float or :class:`~datetime.timedelta`,
Seconds = Union[timedelta, float]


class Bucket(AsyncContextManager):
    """Bucket type.

    A bucket "pours" tokens at a rate of ``rate`` per second (or over').

    Calling `bucket.pour()`, pours one token by default, and returns
    :const:`True` if that amount can be poured now, or :const:`False` if the
    caller has to wait.

    If this returns :const:`False`, it's prudent to either sleep or raise
    an exception::

        if not bucket.pour():
            await asyncio.sleep(bucket.expected_time())

    If you want to consume multiple tokens in one go then specify the number::

        if not bucket.pour(10):
            await asyncio.sleep(bucket.expected_time(10))

    This class can also be used as an async. context manager, but in that case
    can only consume one tokens at a time::

        async with bucket:
            # do something

    By default the async. context manager will suspend the current coroutine
    and sleep until as soon as the time that a token can be consumed.

    If you wish you can also raise an exception, instead of sleeping, by
    providing the ``raises`` keyword argument::

        # hundred tokens in one second, and async with: raises TimeoutError

        class MyError(Exception):
            pass

        bucket = Bucket(100, over=1.0, raises=MyError)

        async with bucket:
            # do something

    """
    rate: float
    fill_rate: float
    capacity: float

    _tokens: float

    def __init__(self, rate: Seconds, over: Seconds = 1.0,
                 *,
                 fill_rate: Seconds = None,
                 capacity: Seconds = None,
                 raises: Type[BaseException] = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.rate = want_seconds(rate)
        self.capacity = want_seconds(over)
        self.raises = raises
        self.loop = loop
        self._tokens = self.capacity
        self.on_init()

    def on_init(self) -> None:
        ...

    @abc.abstractmethod
    def pour(self, tokens: int = 1) -> bool:
        ...

    @abc.abstractmethod
    def expected_time(self, tokens: int = 1) -> float:
        ...

    @property
    @abc.abstractmethod
    def tokens(self) -> float:
        ...

    @property
    def fill_rate(self) -> float:
        #: Defaults to rate! If you want the bucket to fill up
        #: faster/slower, then just override this.
        return self.rate

    async def __aenter__(self) -> 'rate_limit':
        if not self.pour():
            if self.raises:
                raise self.raises()
            await asyncio.sleep(self.expected_time(), loop=self.loop)
        return self

    async def __aexit__(self,
                        exc_type: Type[BaseException] = None,
                        exc_val: BaseException = None,
                        exc_tb: TracebackType = None) -> Optional[bool]:
        return None


class TokenBucket(Bucket):
    _tokens: float
    _last_pour: float

    def on_init(self) -> None:
        self._last_pour = monotonic()

    def pour(self, tokens: int = 1) -> bool:
        need = tokens
        have = self.tokens
        need_to_wait = False
        if need <= have:
            self._tokens -= need
            need_to_wait = True
        return need_to_wait

    def expected_time(self, tokens: int = 1) -> float:
        have = self._tokens
        need = max(tokens, have)
        time_left = (need - have) / self.fill_rate
        return max(time_left, 0.0)

    @property
    def tokens(self) -> float:
        if self._tokens < self.capacity:
            now = monotonic()
            delta = self.fill_rate * (now - self._last_pour)
            self._tokens = min(self.capacity, self._tokens + delta)
            self._last_pour = now
        return self._tokens



def rate_limit(rate: float, over: Seconds = 1.0,
               *,
               bucket_type: Type[Bucket] = TokenBucket,
               raises: Type[BaseException] = None,
               loop: asyncio.AbstractEventLoop = None) -> Bucket:
    return bucket_type(rate, over, raises=raises, loop=loop)


@singledispatch
def want_seconds(s: float) -> float:
    """Convert :data:`Seconds` to float."""
    return s


@want_seconds.register(timedelta)
def _(s: timedelta) -> float:
    return s.total_seconds()


__flake8_TracebackType_is_used: TracebackType  # XXX flake8 bug
__flake8_Type_is_used: Type
