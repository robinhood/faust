import asyncio
from collections import OrderedDict
from typing import Iterator, MutableMapping, Sequence
from itertools import count
from . import constants
from .streams import Stream
from .task import Task
from .types import AppT

DEFAULT_SERVER = 'localhost:9092'


class App(AppT):
    """Faust Application.

    Keyword Arguments:
        servers: List of server host/port pairs.
            Default is ``["localhost:9092"]``.
        loop: Provide specific asyncio event loop instance.
    """

    _index: Iterator[int] = count(0)
    _streams: MutableMapping[str, Stream]

    def __init__(self,
                 servers: Sequence[str] = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self.servers = servers or [DEFAULT_SERVER]
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
        if stream.name in self._streams:
            raise ValueError(
                'Stream with name {0.name!r} already exists.'.format(stream))
        self._streams[stream.name] = stream

    def new_stream_name(self) -> str:
        return self._new_name(constants.SOURCE_NAME)

    def _new_name(self, prefix: str) -> str:
        return '{0}{1:010d}'.format(prefix, next(self._index))
