"""Streaming primitives."""
import faust
from typing import AsyncIterator, Union, cast
from .types import Event, Topic

__all__ = ['through']


async def through(s: AsyncIterator[Event],
                  topic: Union[Topic, str]) -> AsyncIterator[Event]:
    new_stream = None
    if isinstance(topic, str):
        topic = cast(Topic, faust.topic(topic))
    async for event in s:
        if new_stream is None:
            new_stream = event.req.app.stream(topic)
        await event.forward(topic)
        yield await new_stream.__anext__()
