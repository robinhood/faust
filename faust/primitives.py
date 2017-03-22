from typing import Iterable, Union
from .types import EventT, StreamT, Topic


async def through(s: StreamT, topic: Union[Topic, str]) -> Iterable[EventT]:
    new_stream = None
    async for event in s:
        if new_stream is None:
            new_stream = event.req.app.stream(topic)
        await event.forward(topic)
        yield await new_stream.__anext__()  # type: ignore
