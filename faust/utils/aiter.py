from typing import (
    Any, AsyncIterable, AsyncIterator, Iterable, Iterator, Tuple, Union,
)

__all__ = ['aenumerate', 'anext']


async def aenumerate(it: AsyncIterator[Any],
                     start: int = 0) -> AsyncIterator[Tuple[int, Any]]:
    """Asynchronous version of ``enumerate``."""
    i = start
    async for item in it:
        yield i, item
        i += 1


class AsyncIterWrapper(AsyncIterator):

    def __init__(self, it: Iterator) -> None:
        self._it = it

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self) -> Any:
        try:
            return next(self._it)
        except StopIteration as exc:
            raise StopAsyncIteration() from exc

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self._it}>'


def aiter(it: Union[AsyncIterable, Iterable]) -> AsyncIterator:
    if isinstance(it, AsyncIterable):
        # XXX mypy thinks AsyncIterable is an iterator, and AsyncIterator is
        # an iterable, so they have mixed them up.  Probably will be fixed at
        # some point.
        return it.__aiter__()  # type: ignore
    elif isinstance(it, Iterable):
        return AsyncIterWrapper(iter(it)).__aiter__()
    raise TypeError(f'{it!r} object is not an iterable')


def anext(it: AsyncIterator, *default: Any) -> Any:
    """``anext(it) -> it.__anext__()``."""
    if default:
        try:
            return it.__anext__()
        except StopAsyncIteration:
            return default[0]
    return it.__anext__()
