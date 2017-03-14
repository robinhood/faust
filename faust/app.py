import asyncio
from collections import OrderedDict
from typing import Any, Awaitable, Callable, Iterator, MutableMapping
from itertools import count
from . import constants
from .event import Event
from .exceptions import ImproperlyConfigured
from .streams import Stream
from . import transport
from .transport.base import Producer, Transport
from .types import AppT, K, Topic

DEFAULT_URL = 'aiokafka://localhost:9092'


class App(AppT):
    """Faust Application.

    Keyword Arguments:
        servers: List of server host/port pairs.
            Default is ``["localhost:9092"]``.
        loop: Provide specific asyncio event loop instance.
    """

    _index: Iterator[int] = count(0)
    _streams: MutableMapping[str, Stream]
    _producer: Producer = None
    _producer_started: bool = False
    _transport: Transport = None

    def __init__(self,
                 url: str = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self.url = url
        if self.url is None:
            raise ImproperlyConfigured('URL must be specified!')
        self._streams = OrderedDict()

    async def __aenter__(self) -> 'App':
        return self

    async def __aexit__(self, *exc_info) -> None:
        if self._producer:
            await self._producer.stop()

    async def send(self, topic: Topic, key: K, event: Event,
                   wait: bool = True) -> Awaitable:
        if topic.key_serializer:
            key = topic.key_serializer(key)
        value: Any = event
        if topic.value_serializer:
            value = topic.value_serializer(value)

        import json
        value = json.dumps(value._asdict())
        import base64
        valueb = base64.b64encode(value.encode())

        producer = self.producer

        if not self._producer_started:
            self._producer_started = True
            print('+STARTING PRODUCER')
            await producer.start()
            print('-STARTING PRODUCER')

        keyb = key.encode() if key else None

        print('+SEND: {0!r} {1!r} {2!r}'.format(topic.topics[0], valueb, key))
        return await (producer.send_and_wait if wait else producer.send)(
            topic.topics[0], keyb, valueb,
        )
        print('-SEND')

    def add_stream(self, stream: Stream) -> Stream:
        return stream.bind(self)

    def add_task(self, task: Callable) -> Stream:
        return asyncio.ensure_future(task)

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

    def _new_producer(self) -> Producer:
        return self.transport.create_producer()

    def _create_transport(self) -> Transport:
        return transport.from_url(self.url, loop=self.loop)

    @property
    def producer(self) -> Producer:
        if self._producer is None:
            self._producer = self._new_producer()
        return self._producer

    @property
    def transport(self) -> Transport:
        if self._transport is None:
            self._transport = self._create_transport()
        return self._transport
