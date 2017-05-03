from typing import Any, AsyncIterator, Tuple

__all__ = ['aenumerate', 'anext']


async def aenumerate(it: AsyncIterator[Any],
                     start: int = 0) -> AsyncIterator[Tuple[int, Any]]:
    """Asynchronous version of ``enumerate``."""
    i = start
    async for item in it:
        yield i, item
        i += 1


def anext(it: AsyncIterator, *default: Any) -> Any:
    """``anext(it) -> it.__anext__()``."""
    if default:
        try:
            return it.__anext__()
        except StopAsyncIteration:
            return default[0]
    return it.__anext__()
