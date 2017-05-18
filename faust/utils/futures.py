"""Async I/O Future utilities."""
import asyncio
from typing import Any, Awaitable

__all__ = ['done_future', 'maybe_async']


def done_future(result: Any = None, *,
                loop: asyncio.AbstractEventLoop = None) -> asyncio.Future:
    """Returns :class:`asyncio.Future` that is already evaluated."""
    f = (loop or asyncio.get_event_loop()).create_future()
    f.set_result(result)
    return f


async def maybe_async(res: Any) -> Any:
    """Await future if argument is awaitable.

    Examples:
        >>> await maybe_async(regular_function(arg))
        >>> await maybe_async(async_function(arg))
    """
    if isinstance(res, Awaitable):
        return await res
    return res
