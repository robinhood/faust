"""Applications."""
import asyncio
import faust
import sys
import typing
from collections import OrderedDict, deque
from typing import (
    Any, Awaitable, Callable, ClassVar, Deque, Iterator, Mapping,
    MutableMapping, Optional, Sequence, Set, Union, Tuple, Type, cast,
)
from itertools import count
from weakref import WeakKeyDictionary
from . import constants
from . import transport
from .codecs import loads
from .exceptions import ImproperlyConfigured, KeyDecodeError, ValueDecodeError
from .streams import StreamManager
from .types import CodecArg, K, Message, Request, TaskArg, Topic, V
from .types.app import AppT, AsyncSerializerT
from .types.collections import NodeT
from .types.coroutines import StreamCoroutine
from .types.models import Event, ModelT
from .types.sensors import SensorT
from .types.services import ServiceT
from .types.streams import Processor, StreamT
from .types.tables import TableT
from .types.transports import ProducerT, TransportT
from .utils.compat import want_bytes
from .utils.futures import Group
from .utils.imports import SymbolArg, symbol_by_name
from .utils.logging import get_logger
from .utils.objects import cached_property
from .utils.services import Service, ServiceProxy

__all__ = ['App']

__flake8_please_Any_is_OK: Any   # flake8 thinks Any is unused :/

DEFAULT_URL = 'kafka://localhost:9092'
DEFAULT_STREAM_CLS = 'faust.Stream'
DEFAULT_TABLE_CLS = 'faust.Table'
CLIENT_ID = 'faust-{0}'.format(faust.__version__)
COMMIT_INTERVAL = 30.0
APP_REPR = """
<{name}({self.id}): {self.url} {self.state} tasks={tasks} streams={streams}>
""".strip()

if typing.TYPE_CHECKING:
    # TODO mypy does not recognize WeakKeyDictionary as a MutableMapping
    TASK_TO_APP: WeakKeyDictionary[asyncio.Task, AppT]
TASK_TO_APP = WeakKeyDictionary()

logger = get_logger(__name__)


class AppService(Service):

    def __init__(self, app: 'App', **kwargs: Any) -> None:
        self.app: App = app
        super().__init__(loop=self.app.loop, **kwargs)

    async def on_start(self) -> None:
        self.app.streams.beacon = self.beacon.new(self.app.streams)
        await self._start_streams()
        await self.app.streams.start()
        await self._start_tasks()

    async def _start_streams(self) -> None:
        for _stream in self.app._streams.values():
            await _stream.maybe_start()

    async def _start_tasks(self) -> None:
        if not len(self.app._tasks):
            raise ImproperlyConfigured(
                'Attempting to start app that has no tasks')
        await self.app._tasks.start()
        # wait for tasks to finish, this may run forever since
        # stream processing tasks do not end (them infinite!)
        await self.app._tasks.joinall()

    async def on_stop(self) -> None:
        await self._stop_streams()
        await self._stop_producer()
        await self.app.streams.stop()

    async def _stop_streams(self) -> None:
        # stop all streams in reversed order.
        for _stream in reversed(list(self.app._streams.values())):
            await _stream.stop()

    async def _stop_producer(self) -> None:
        if self.app._producer is not None:
            await self.app._producer.stop()

    @property
    def label(self) -> str:
        return '{name}: {app.id}@{app.url}'.format(
            name=type(self.app).__name__,
            app=self.app,
        )


class App(AppT, ServiceProxy):
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

    #: Used for generating new topic names.
    _index: ClassVar[Iterator[int]] = count(0)

    #: Mapping of active streams by name.
    _streams: MutableMapping[str, StreamT]

    #: Mapping of active tables by name.
    _tables: MutableMapping[str, TableT]

    #: Group creates the event loop, so we instantiate it lazily
    #: only when something accesses ._tasks
    _tasks_group: Group = None

    #: Default producer instance.
    _producer: Optional[ProducerT] = None

    #: Set when producer is started.
    _producer_started: bool = False

    #: Transport is created on demand: use `.transport`.
    _transport: Optional[TransportT] = None

    _serializer_override_classes: Mapping[CodecArg, SymbolArg] = {
        'avro': 'faust.utils.avro.faust:AvroSerializer',
    }
    _serializer_override: MutableMapping[CodecArg, AsyncSerializerT] = None

    _message_buffer: Deque[Tuple[Union[Topic, str], K, V]]

    #: Set of active sensors
    _sensors: Set[SensorT] = None

    @classmethod
    def current_app(self) -> AppT:
        return TASK_TO_APP[asyncio.Task.current_task(loop=self.loop)]

    def __init__(self, id: str,
                 *,
                 url: str = 'aiokafka://localhost:9092',
                 client_id: str = CLIENT_ID,
                 commit_interval: float = COMMIT_INTERVAL,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = 'json',
                 num_standby_replicas: int = 0,
                 replication_factor: int = 1,
                 avro_registry_url: str = None,
                 stream_cls: SymbolArg = DEFAULT_STREAM_CLS,
                 table_cls: SymbolArg = DEFAULT_TABLE_CLS,
                 store: str = 'memory://',
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop
        self.id = id
        self.url = url
        self.client_id = client_id
        self.commit_interval = commit_interval
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.num_standby_replicas = num_standby_replicas
        self.replication_factor = replication_factor
        self.avro_registry_url = avro_registry_url
        self.Stream = symbol_by_name(stream_cls)
        self.Table = symbol_by_name(table_cls)
        self.store = store
        self._streams = OrderedDict()
        self._tables = OrderedDict()
        self._message_buffer = deque()
        self._serializer_override = {}
        self._sensors = set()
        self.streams = StreamManager(app=self)

    async def send(
            self, topic: Union[Topic, str], key: K, value: V,
            *,
            wait: bool = True) -> Awaitable:
        """Send event to stream.

        Arguments:
            topic (Union[Topic, str]): Topic to send event to.
            key (K): Message key.
            value (V): Message value.

        Keyword Arguments:
            wait (bool): Wait for message to be published (default),
                if unset the message will only be appended to the buffer.
        """
        if isinstance(topic, Topic):
            strtopic = topic.topics[0]
        return await self._send(
            strtopic,
            (await self.dumps_key(strtopic, key) if key is not None else None),
            (await self.dumps_value(strtopic, value)),
            wait=wait,
        )

    def send_soon(self, topic: Union[Topic, str], key: K, value: V) -> None:
        self._message_buffer.append((topic, key, value))

    async def _send(self,
                    topic: str,
                    key: Optional[bytes],
                    value: Optional[bytes],
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

    async def loads_key(self, typ: Optional[Type], key: bytes) -> K:
        if key is None or typ is None:
            return key
        try:
            typ_serializer = typ._options.serializer  # type: ignore
            serializer = typ_serializer or self.key_serializer
            try:
                ser = self._get_serializer(serializer)
            except KeyError:
                obj = loads(serializer, key)
            else:
                obj = await ser.loads(key)
            return cast(K, typ(obj))
        except Exception as exc:
            raise KeyDecodeError(str(exc)).with_traceback(sys.exc_info()[2])

    async def loads_value(self, typ: Type, key: K, message: Message) -> Event:
        if message.value is None:
            return None
        try:
            obj: Any = None
            typ_serializer = typ._options.serializer  # type: ignore
            serializer = typ_serializer or self.value_serializer
            try:
                ser = self._get_serializer(serializer)
            except KeyError:
                obj = loads(serializer, message.value)
            else:
                obj = await ser.loads(message.value)
            return cast(Event, typ(obj, req=Request(self, key, message)))
        except Exception as exc:
            raise ValueDecodeError(str(exc)).with_traceback(sys.exc_info()[2])

    async def dumps_key(self, topic: str, key: K) -> Optional[bytes]:
        if isinstance(key, ModelT):
            try:
                ser = self._get_serializer(key._options.serializer)
            except KeyError:
                return key.dumps()
            else:
                return await ser.dumps_key(topic, key)
        return want_bytes(key) if key is not None else key

    async def dumps_value(self, topic: str, value: V) -> Optional[bytes]:
        if isinstance(value, ModelT):
            try:
                ser = self._get_serializer(value._options.serializer)
            except KeyError:
                return value.dumps()
            else:
                return await ser.dumps_value(topic, value)
        return value

    def _get_serializer(self, name: CodecArg) -> AsyncSerializerT:
        # Caches overridden AsyncSerializer
        # e.g. the avro serializer communicates with a Schema registry
        # server, so it needs to be async.
        # See app.dumps_key, .dumps_value, .loads_key, .loads_value,
        # and the AsyncSerializer implementation in
        #   faust/utils/avro/faust.py
        try:
            return self._serializer_override[name]
        except KeyError:
            ser = self._serializer_override[name] = (
                symbol_by_name(self._serializer_override_classes[name])(self))
            return cast(AsyncSerializerT, ser)

    def add_task(self, task: TaskArg) -> Awaitable:
        """Start task.

        Notes:
            A task is simply any coroutine iterating over a stream.
        """
        return self._tasks.add(task)

    async def _on_task_started(
            self, task: asyncio.Task,
            *, _set: Callable = TASK_TO_APP.__setitem__) -> None:
        _set(task, self)  # add to TASK_TO_APP mapping

    async def _on_task_error(self, task: asyncio.Task, exc: Exception) -> None:
        try:
            await self.streams.consumer.on_task_error(exc)
        except Exception as exc:
            logger.exception('Consumer error callback raised: %r', exc)

    async def _on_task_stopped(self, task: asyncio.Task) -> None:
        ...

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
            beacon=self.beacon,
            **kwargs
        ).bind(self)

    def table(self, table_name: str,
              *,
              default: Callable[[], Any] = None,
              topic: Topic = None,
              coroutine: StreamCoroutine = None,
              processors: Sequence[Processor] = None,
              **kwargs: Any) -> TableT:
        Table = cast(TableT, self.Table)
        return cast(TableT, Table.from_topic(
            topic,
            table_name=table_name,
            default=default,
            coroutine=coroutine,
            processors=processors,
            beacon=self.beacon,
            **kwargs
        ).bind(self))

    def add_sensor(self, sensor: SensorT) -> None:
        # connect beacons
        sensor.beacon = self.beacon.new(sensor)
        self._sensors.add(sensor)

    def remove_sensor(self, sensor: SensorT) -> None:
        self._sensors.remove(sensor)

    async def on_message_in(
            self,
            consumer_id: int,
            offset: int,
            message: Message) -> None:
        for _sensor in self._sensors:
            await _sensor.on_message_in(consumer_id, offset, message)

    async def on_message_out(
            self,
            consumer_id: int,
            offset: int,
            message: Message = None) -> None:
        for _sensor in self._sensors:
            await _sensor.on_message_out(consumer_id, offset, message)

    def add_source(self, stream: StreamT) -> None:
        """Register existing stream."""
        assert stream.name
        if stream.name in self._streams:
            raise ValueError(
                'Stream with name {0.name!r} already exists.'.format(stream))
        self._streams[stream.name] = stream
        self.streams.add_stream(stream)

    def add_table(self, table: TableT) -> None:
        """Register existing table."""
        assert table.table_name
        if table.table_name in self._tables:
            raise ValueError(
                'Table with name {0.table_name!r} already exists'.format(
                    table))
        self._tables[table.table_name] = table

    def new_stream_name(self) -> str:
        """Create a new name for a stream."""
        return self._new_name(constants.SOURCE_NAME)

    def _new_name(self, prefix: str) -> str:
        return '{0}{1:010d}'.format(prefix, next(self._index))

    def _new_producer(self, beacon: NodeT = None) -> ProducerT:
        return self.transport.create_producer(
            beacon=beacon or self.beacon,
        )

    def _create_transport(self) -> TransportT:
        return cast(TransportT,
                    transport.by_url(self.url)(self.url, self, loop=self.loop))

    def _new_group(self, **kwargs: Any) -> Group:
        return Group(beacon=self.beacon, **kwargs)

    def __repr__(self) -> str:
        return APP_REPR.format(
            name=type(self).__name__,
            self=self,
            tasks=self.tasks_running,
            streams=len(self._streams),
        )

    @cached_property
    def _tasks(self) -> Group:
        return self._new_group(
            on_task_started=self._on_task_started,
            on_task_error=self._on_task_error,
            on_task_stopped=self._on_task_stopped,
            loop=self.loop,
        )

    @property
    def producer(self) -> ProducerT:
        if self._producer is None:
            self._producer = self._new_producer()
        return self._producer

    @producer.setter
    def producer(self, producer: ProducerT) -> None:
        self._producer = producer

    @property
    def transport(self) -> TransportT:
        if self._transport is None:
            self._transport = self._create_transport()
        return self._transport

    @transport.setter
    def transport(self, transport: TransportT) -> None:
        self._transport = transport

    @property
    def tasks_running(self) -> int:
        return len(self._tasks)

    @cached_property
    def _service(self) -> ServiceT:
        return AppService(self)
