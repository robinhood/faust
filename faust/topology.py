import asyncio
from collections import OrderedDict
from itertools import count
from typing import MutableMapping
from . import constants
from .streams import Stream
from .task import Task
from .types import Topic
from .utils.service import Service


class Topology(Service):

    _index = count(0)
    _streams: MutableMapping[str, Stream]

    def __init__(self, loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self._streams = OrderedDict()

    def add_task(self, task: Task) -> None:
        ...

    async def on_start(self) -> None:
        for _stream in self._streams.values():
            await _stream.start()

    async def on_stop(self) -> None:
        for _stream in self._streams.values():
            await _stream.stop()

    def stream(self, topic: Topic,) -> Stream:
        stream = Stream(
            self._new_name(constants.SOURCE_NAME),
            topic=topic,
        )
        self.add_source(stream)
        return stream

    def add_source(self, stream):
        assert stream.name
        if not stream.pattern:
            assert stream.topic
        if stream.name in self._streams:
            raise ValueError(
                'Stream with name {0.name!r} already exists.'.format(stream))
        self._streams[stream.name] = stream

    def _new_name(self, prefix: str) -> str:
        return '{0}{1:010d}'.format(prefix, next(self._index))
