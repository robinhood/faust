"""Streaming primitives."""
import faust
from typing import AsyncIterable, List, Optional, Union, cast
from .types import Event, StreamT, Topic

__all__ = ['through']


async def through(
        s: StreamT, topic: Union[Topic, str]) -> AsyncIterable[Event]:
    if isinstance(topic, str):
        topic = faust.topic(topic)
    topic = cast(Topic, topic)
    new_stream: List[Optional[StreamT]] = [None]
    return (await _do_through(topic, new_stream, event) async for event in s)


async def _do_through(topic: Topic,
                      new_stream: List[Optional[StreamT]],
                      event: Event):
    if new_stream[0] is None:
        new_stream[0] = event.req.app.stream(topic)
    await event.forward(topic)
    return await new_stream[0].__anext__()
