from typing import Any, AsyncIterator, Tuple


async def aenumerate(it: AsyncIterator[Any],
                     start: int = 0) -> AsyncIterator[Tuple[int, Any]]:
    i = start
    async for item in it:
        yield i, item
        i += 1
