import asyncio
from typing import Any

__all__ = ['done_future']


def done_future(result: Any = None, *,
                loop: asyncio.AbstractEventLoop = None) -> asyncio.Future:
    f = (loop or asyncio.get_event_loop()).create_future()
    f.set_result(result)
    return f
