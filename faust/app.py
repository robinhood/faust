import aiokafka
import asyncio
from collections import OrderedDict
from typing import Any, Awaitable, Iterator, MutableMapping, Sequence
from itertools import count
from . import constants
from .event import Event
from .streams import Stream
from .task import Task
from .types import AppT, K, Topic

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
    _producer: aiokafka.AIOKafkaProducer = None
    _producer_started: bool = False

    def __init__(self,
                 servers: Sequence[str] = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self.servers = servers or [DEFAULT_SERVER]
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

        print('+SEND: {0!r} {1!r} {2!r}'.format(topic.topics[0], valueb, key))
        return await (producer.send_and_wait if wait else producer.send)(
            topic.topics[0], valueb,
            key=key,
        )
        print('-SEND')

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

    def _new_producer(self) -> aiokafka.AIOKafkaProducer:
        return aiokafka.AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=self.servers[0],
        )

    @property
    def producer(self) -> aiokafka.AIOKafkaProducer:
        if self._producer is None:
            self._producer = self._new_producer()
        return self._producer
