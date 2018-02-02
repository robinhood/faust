"""Async iterator lost and found missing methods: aiter, anext, etc."""
import collections
from functools import singledispatch
from typing import Any, AsyncIterable, AsyncIterator, Iterable, Iterator, Tuple

__all__ = ['aenumerate', 'aiter', 'anext']


async def aenumerate(it: AsyncIterable[Any],
                     start: int = 0) -> AsyncIterator[Tuple[int, Any]]:
    """``async for`` version of ``enumerate``."""
    i = start
    async for item in it:
        yield i, item
        i += 1


class AsyncIterWrapper(AsyncIterator):
    """Wrap regular Iterator into an AsyncIterator."""

    def __init__(self, it: Iterator) -> None:
        self._it: Iterator = it

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self) -> Any:
        try:
            return next(self._it)
        except StopIteration as exc:
            raise StopAsyncIteration() from exc

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self._it}>'


@singledispatch
def aiter(it: Any) -> AsyncIterator:
    """Create iterator from iterable.

    Notes:
        If the object is already an iterator, the iterator
        should return self when ``__aiter__`` is called.
    """
    raise TypeError(f'{it!r} object is not an iterable')


# XXX In Py3.7: register cannot take typing.AsyncIterator
@aiter.register(collections.AsyncIterable)
def _aiter_async(it: AsyncIterable) -> AsyncIterator:
    return it.__aiter__()


# XXX In Py3.7: register cannot take typing.Iterable
@aiter.register(collections.Iterable)
def _aiter_iter(it: Iterable) -> AsyncIterator:
    return AsyncIterWrapper(iter(it)).__aiter__()


async def anext(it: AsyncIterator, *default: Any) -> Any:
    """Get next value from async iterator, or `default` if empty.

    Raises
        :exc:`StopAsyncIteration`: if default is not defined and
            the async iterator is fully consumed.
    """
    if default:
        try:
            return await it.__anext__()
        except StopAsyncIteration:
            return default[0]
    return await it.__anext__()
