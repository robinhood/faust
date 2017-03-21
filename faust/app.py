import asyncio
import faust
from collections import OrderedDict
from typing import (
    Any, Awaitable, Generator, Iterator, MutableMapping, Union,
    cast,
)
from itertools import count
from . import constants
from . import transport
from .event import Event
from .exceptions import ImproperlyConfigured
from .streams import Stream
from .transport.base import Producer, Transport
from .types import AppT, K, SerializerArg, Topic
from .utils.log import get_logger
from .utils.serialization import dumps
from .utils.service import Service

__foobar: Any   # flake8 thinks Any is unused for some reason

CLIENT_ID = 'faust-{0}'.format(faust.__version__)
DEFAULT_URL = 'aiokafka://localhost:9092'

logger = get_logger(__name__)


class App(AppT, Service):
    """Faust Application.

    Keyword Arguments:
        url (str):
            Transport URL, for example: ``"aiokafka://localhost:9092"``.
        loop (asyncio.AbstractEventLoop):
            Provide specific asyncio event loop instance.
    """

    id: str
    url: str
    client_id: str
    loop: asyncio.AbstractEventLoop

    #: Used for generating new topic names.
    _index: Iterator[int] = count(0)

    #: Mapping of active streams by name.
    _streams: MutableMapping[str, Stream]

    #: Default producer instance.
    _producer: Producer = None

    #: Set when producer is started.
    _producer_started: bool = False

    #: Transport is created on demand: use `.transport`.
    _transport: Transport = None

    def __init__(self, id: str,
                 *,
                 url: str = 'aiokafka://localhost:9092',
                 client_id: str = CLIENT_ID,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        super().__init__(loop=loop or asyncio.get_event_loop())
        self.id = id
        self.client_id = client_id
        self.url = url
        if self.url is None:
            raise ImproperlyConfigured('URL must be specified!')
        self._streams = OrderedDict()

    async def send(
            self, topic: Union[Topic, str], key: K, event: Event,
            *,
            wait: bool = True,
            key_serializer: SerializerArg = None) -> Awaitable:
        """Send event to stream.

        Arguments:
            topic (Union[Topic, str]): Topic to send event to.
            key (Any): Message key.
            event (Event): Message value.

        Keyword Arguments:
            wait (bool): Wait for message to be published (default),
                if unset the message will only be appended to the buffer.
        """
        if isinstance(topic, Topic):
            topic = cast(Topic, topic)
            key_serializer = key_serializer or topic.key_serializer
            strtopic = topic.topics[0]
        else:
            strtopic = cast(str, topic)
        if key_serializer:
            key = dumps(key_serializer, key)
        value: Any = event.dumps()

        return await self._send(
            strtopic,
            key.encode() if key else None,
            value.encode() if value else None,
        )

    async def _send(self, topic: str, key: bytes, value: bytes,
                    *,
                    wait: bool = True) -> Awaitable:
        logger.debug('send: topic=%r key=%r value=%r', topic, key, value)
        producer = self.producer
        if not self._producer_started:
            self._producer_started = True
            await producer.start()
        return await (producer.send_and_wait if wait else producer.send)(
            topic, key, value,
        )

    def add_stream(self, stream: Stream) -> Stream:
        """Instantiate stream to be run within the context of this app.

        Returns:
            Stream: new instance of stream bound to this app.
        """
        return stream.bind(self)

    def add_task(self, task: Union[Generator, Awaitable]) -> asyncio.Future:
        """Start task.

        Notes:
            A task is simply any coroutine taking one or more streams
            as argument and iterating over them, so currently `add_task`
            is simply scheduling the coroutine to be executed in the event
            loop.
        """
        return asyncio.ensure_future(task, loop=self.loop)

    def stream(self, topic: Topic, **kwargs) -> Stream:
        """Create new stream from topic.

        Returns:
            faust.streams.Stream:
                to iterate over events in the stream.
        """
        return self.add_stream(Stream(topics=[topic], **kwargs))

    async def on_start(self) -> None:
        for _stream in self._streams.values():
            await _stream.start()

    async def on_stop(self) -> None:
        for _stream in self._streams.values():
            await _stream.stop()
        if self._producer:
            await self._producer.stop()

    def add_source(self, stream: Stream) -> None:
        """Register existing stream."""
        assert stream.name
        if stream.name in self._streams:
            raise ValueError(
                'Stream with name {0.name!r} already exists.'.format(stream))
        self._streams[stream.name] = stream

    def new_stream_name(self) -> str:
        """Create a new name for a stream."""
        return self._new_name(constants.SOURCE_NAME)

    def _new_name(self, prefix: str) -> str:
        return '{0}{1:010d}'.format(prefix, next(self._index))

    def _new_producer(self) -> Producer:
        return self.transport.create_producer()

    def _create_transport(self) -> Transport:
        return transport.from_url(self, self.url, loop=self.loop)

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
