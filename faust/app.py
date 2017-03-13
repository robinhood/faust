import asyncio
from collections import OrderedDict
from typing import MutableMapping
from itertools import count
from . import constants
from .task import Task
from .types import AppT

if 0:
    from .stream import Stream


class App(AppT):

    _index = count(0)
    _streams: MutableMapping[str, Stream]

    def __init__(self, loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self._streams = OrderedDict()

    def add_stream(self, stream: Stream) -> Stream:
        return stream.bind(self)

    def add_task(self, task: Task) -> Stream:
        ...

    async def on_start(self) -> None:
        for _stream in self._streams.values():
            await _stream.start()

    async def on_stop(self) -> None:
        for _stream in self._streams.values():
            await _stream.stop()

    def add_source(self, stream: Stream) -> None:
        assert stream.name
        if not stream.pattern:
            assert stream.topic
        if stream.name in self._streams:
            raise ValueError(
                'Stream with name {0.name!r} already exists.'.format(stream))
        self._streams[stream.name] = stream

    def new_stream_name(self) -> str:
        return self._new_name(constants.SOURCE_NAME)

    def _new_name(self, prefix: str) -> str:
        return '{0}{1:010d}'.format(prefix, next(self._index))
