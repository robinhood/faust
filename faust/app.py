"""Applications."""
import asyncio
import faust
from collections import OrderedDict, deque
from typing import (
    Any, Awaitable, Iterator, MutableMapping,
    Optional, Sequence, Union, Type, cast,
)
from itertools import count
from . import constants
from . import transport
from .codecs import dumps
from .types import (
    AppT, CodecArg, K, ModelT, Processor, ProducerT,
    StreamCoroutine, StreamT, TaskArg, Topic, TransportT, V,
)
from .utils.compat import want_bytes
from .utils.imports import symbol_by_name
from .utils.logging import get_logger
from .utils.services import Service

__all__ = ['App']

__flake8_please_Any_is_OK: Any   # flake8 thinks Any is unused :/

DEFAULT_URL = 'kafka://localhost:9092'
DEFAULT_STREAM_CLS = 'faust.Stream'
CLIENT_ID = 'faust-{0}'.format(faust.__version__)
COMMIT_INTERVAL = 30.0

logger = get_logger(__name__)


class TaskExceptionHandler:

    app: AppT

    def __init__(self, app: AppT) -> None:
        self.app = app

    async def __aenter__(self) -> 'TaskExceptionHandler':
        return self

    def __aexit__(
            self, typ: Type, exc: Exception, tb: Any) -> None:
        if typ is not None:
            logger.exception('Task raised exception: %r', exc)
            # XXX Rollback automatic acks.


class App(AppT, Service):
    """Faust Application.

    Arguments:
        id (str): Application ID.

    Keyword Arguments:
        url (str):
            Transport URL.  Default: ``"aiokafka://localhost:9092"``.
        client_id (str):  Client id used for producer/consumer.
        commit_interval (float): How often we commit offset when automatic
            commit is enabled.  Default ``30.0``.
        key_serializer (CodecArg): Default serializer for Topics
            that do not have an explicit serializer set.
            Default: :const:`None`.
        value_serializer (CodecArg): Default serializer for event types
            that do not have an explicit serializer set.  Default: ``"json"``.
        num_standby_replicas (int): The number of standby replicas for each
            task.  Default: ``0``.
        replication_factor (int): The replication factor for changelog topics
            and repartition topics created by the application.  Default: ``1``.
        loop (asyncio.AbstractEventLoop):
            Provide specific asyncio event loop instance.
    """
    TaskExceptionHandler: Type = TaskExceptionHandler

    #: Used for generating new topic names.
    _index: Iterator[int] = count(0)

    #: Mapping of active streams by name.
    _streams: MutableMapping[str, StreamT]

    #: List of tasks
    _tasks: deque

    #: Default producer instance.
    _producer: Optional[ProducerT] = None

    #: Set when producer is started.
    _producer_started: bool = False

    #: Transport is created on demand: use `.transport`.
    _transport: Optional[TransportT] = None

    def __init__(self, id: str,
                 *,
                 url: str = 'aiokafka://localhost:9092',
                 client_id: str = CLIENT_ID,
                 commit_interval: float = COMMIT_INTERVAL,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = 'json',
                 num_standby_replicas: int = 0,
                 replication_factor: int = 1,
                 stream_cls: Union[Type, str] = DEFAULT_STREAM_CLS,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        super().__init__(loop=loop or asyncio.get_event_loop())
        self.id = id
        self.client_id = client_id
        self.commit_interval = commit_interval
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.num_standby_replicas = num_standby_replicas
        self.replication_factor = replication_factor
        self.url = url
        self.Stream = symbol_by_name(stream_cls)
        self._streams = OrderedDict()
        self._tasks = deque()

    async def send(
            self, topic: Union[Topic, str], key: K, value: V,
            *,
            wait: bool = True,
            key_serializer: CodecArg = None) -> Awaitable:
        """Send event to stream.

        Arguments:
            topic (Union[Topic, str]): Topic to send event to.
            key (K): Message key.
            value (V): Message value.

        Keyword Arguments:
            wait (bool): Wait for message to be published (default),
                if unset the message will only be appended to the buffer.
        """
        key_bytes: Optional[bytes] = None
        if isinstance(topic, Topic):
            key_serializer = key_serializer or topic.key_serializer
            strtopic = topic.topics[0]
        if key is not None:
            if isinstance(key, ModelT):
                key_bytes = key.dumps()
            elif key_serializer is not None:
                key_bytes = dumps(key_serializer, key)
            else:
                key_bytes = want_bytes(key)
        value_bytes: bytes = value.dumps()

        return await self._send(
            strtopic,
            key_bytes,
            value_bytes,
            wait=wait,
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

    def add_task(self, task: TaskArg) -> asyncio.Future:
        """Start task.

        Notes:
            A task is simply any coroutine iterating over a stream.
        """
        fut = self._start_task(task)
        self._tasks.append(fut)
        return fut

    async def _start_task(self, task):
        await self._execute_task(task)

    async def _execute_task(self, task: TaskArg) -> None:
        async with self.TaskExceptionHandler(self):
            await asyncio.ensure_future(task, loop=self.loop)

    def stream(self, topic: Topic,
               coroutine: StreamCoroutine = None,
               processors: Sequence[Processor] = None,
               **kwargs: Any) -> StreamT:
        """Create new stream from topic.

        Arguments:
            topic (Topic): Topic description to consume from.

        Keyword Arguments:
            coroutine (Callable): Coroutine to filter events in this stream.
            processors (Sequence[Callable]): List of processors for events in
                this stream.

        Returns:
            faust.Stream:
                to iterate over events in the stream.
        """
        return cast(StreamT, self.Stream).from_topic(
            topic,
            coroutine=coroutine,
            processors=processors,
            **kwargs
        ).bind(self)

    async def on_start(self) -> None:
        for _stream in self._streams.values():  # start all streams
            await _stream.start()
        while self._tasks:
            await asyncio.ensure_future(self._tasks.popleft(), loop=self.loop)

    async def on_stop(self) -> None:
        # stop all streams
        for _stream in reversed(list(self._streams.values())):
            await _stream.stop()
        # stop producer
        if self._producer is not None:
            await self._producer.stop()

    def add_source(self, stream: StreamT) -> None:
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

    def _new_producer(self) -> ProducerT:
        return self.transport.create_producer()

    def _create_transport(self) -> TransportT:
        return transport.from_url(self.url, self, loop=self.loop)

    @property
    def producer(self) -> ProducerT:
        if self._producer is None:
            self._producer = self._new_producer()
        return cast(ProducerT, self._producer)

    @producer.setter
    def producer(self, producer: ProducerT) -> None:
        self._producer = producer

    @property
    def transport(self) -> TransportT:
        if self._transport is None:
            self._transport = self._create_transport()
        return cast(TransportT, self._transport)

    @transport.setter
    def transport(self, transport: TransportT) -> None:
        self._transport = transport
