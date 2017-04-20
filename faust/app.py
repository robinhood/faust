"""Applications."""
import asyncio
import faust
import io
import sys
import typing

from collections import OrderedDict
from typing import (
    Any, Awaitable, Callable, ClassVar, Generator, Iterator,
    Mapping, MutableMapping, MutableSequence, Optional, Sequence,
    Set, Union, Type, cast,
)
from itertools import count
from weakref import WeakKeyDictionary

from . import transport
from .exceptions import ImproperlyConfigured, KeyDecodeError, ValueDecodeError
from .serializers.codecs import CodecArg, loads
from .streams import _constants
from .streams.manager import StreamManager
from .types import K, Message, Request, StreamCoroutine, Topic, V
from .types.app import AppT, AsyncSerializerT
from .types.models import Event, ModelT
from .types.sensors import SensorT
from .types.streams import Processor, StreamT, StreamManagerT
from .types.tables import TableT
from .types.transports import ProducerT, TransportT
from .types.windows import WindowT
from .utils.compat import want_bytes
from .utils.futures import Group
from .utils.imports import SymbolArg, symbol_by_name
from .utils.logging import get_logger
from .utils.objects import cached_property
from .utils.services import Service, ServiceProxy, ServiceT
from .utils.types.collections import NodeT
from .web import Web

__all__ = ['App']

__flake8_please_Any_is_OK: Any   # flake8 thinks Any is unused :/

#: Default broker URL.
DEFAULT_URL = 'kafka://localhost:9092'

#: Path to default stream class used by ``app.stream``.
DEFAULT_STREAM_CLS = 'faust.Stream'

#: Path to default table class used by ``app.table``.
DEFAULT_TABLE_CLS = 'faust.Table'

#: Path to default Web site class.
DEFAULT_WEBSITE_CLS = 'faust.web.site:create_site'

#: Default Kafka Client ID.
CLIENT_ID = 'faust-{0}'.format(faust.__version__)

#: How often we commit messages.
#: Can be customized by setting ``App(commit_interval=...)``.
COMMIT_INTERVAL = 30.0

#: Format string for ``repr(app)``.
APP_REPR = """
<{name}({self.id}): {self.url} {self.state} tasks={tasks} streams={streams}>
""".strip()

if typing.TYPE_CHECKING:
    # TODO mypy does not recognize WeakKeyDictionary as a MutableMapping
    TASK_TO_APP: WeakKeyDictionary[asyncio.Task, AppT]
#: Map asyncio.Task to the app that started it.
#: Tasks can use ``App.current_app`` to get the "currently used app".
TASK_TO_APP = WeakKeyDictionary()

logger = get_logger(__name__)


class AppService(Service):
    """Service responsible for starting/stopping an application."""

    # App is created in module scope so we split it up to ensure
    # Service.loop does not create the asyncio event loop
    # when a module is imported.

    def __init__(self, app: 'App', **kwargs: Any) -> None:
        self.app: App = app
        super().__init__(loop=self.app.loop, **kwargs)

    def on_init_dependencies(self) -> Sequence[ServiceT]:
        for task in self.app._task_factories:
            self.app.add_task(task(self.app))
        streams = list(self.app._streams.values())    # Stream+Table instances
        services = [
            self.app.producer,                        # app.Producer
            self.app.website,                        # app.Web
            self.app.streams,                         # app.StreamManager
            self.app._tasks,                          # app.Group
        ]
        return cast(Sequence[ServiceT], streams + services)

    async def on_first_start(self) -> None:
        if not len(self.app._tasks):
            raise ImproperlyConfigured(
                'Attempting to start app that has no tasks')

    async def on_start(self) -> None:
        self.add_future(self._drain_message_buffer())

    async def on_started(self) -> None:
        if self.app.beacon.root:
            try:
                callback = self.app.beacon.root.data.on_startup_finished
            except AttributeError:
                pass
            else:
                callback()
        # wait for tasks to finish, this may run forever since
        # stream processing tasks do not end (them infinite!)
        await self.app._tasks.joinall()

    async def _drain_message_buffer(self) -> None:
        send = self.app.send
        get = self.app._message_buffer.get
        while not self.should_stop:
            topic, key, value = await get()
            send(topic, key, value)

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

    #: Used for generating internal names (see new_name taken from KS).
    _index: ClassVar[Iterator[int]] = count(0)

    #: Mapping of active streams by name.
    _streams: MutableMapping[str, StreamT]

    #: Mapping of active tables by table name.
    _tables: MutableMapping[str, TableT]

    #: The ._tasks Group starts and stops tasks in the app.
    #: Creating a group also creates the event loop, so _tasks is a property
    #: that sets _tasks_group lazily.
    _tasks_group: Group = None

    #: Default producer instance.
    _producer: Optional[ProducerT] = None

    #: Set when producer is started.
    _producer_started: bool = False

    #: Transport is created on demand: use `.transport`.
    _transport: Optional[TransportT] = None

    #: Mapping of serializers that needs to be async
    _serializer_override_classes: Mapping[CodecArg, SymbolArg] = {
        'avro': 'faust.serializers.avro.faust:AvroSerializer',
    }

    #: Async serializer instances are cached here.
    _serializer_override: MutableMapping[CodecArg, AsyncSerializerT] = None

    #: Set of active sensors
    _sensors: Set[SensorT] = None

    #: The @app.task decorator adds tasks to start here.
    #: for example:
    #:     app = faust.App('myid', tasks=[task])
    #:     >>> @app.task
    #:     def mytask(app):
    #:     ...     ...
    _task_factories: MutableSequence[Callable[['AppT'], Generator]]

    @classmethod
    def current_app(self) -> AppT:
        """Returns the app that created the active task."""
        return TASK_TO_APP[asyncio.Task.current_task(loop=self.loop)]

    def start(self, *,
              argv: Sequence[str] = None,
              loop: asyncio.AbstractEventLoop = None) -> None:
        from .bin.worker import worker
        from .worker import Worker
        kwargs = worker(argv, standalone_mode=False)
        Worker(self, loop=loop, **kwargs).execute_from_commandline()

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
                 Stream: SymbolArg = DEFAULT_STREAM_CLS,
                 Table: SymbolArg = DEFAULT_TABLE_CLS,
                 WebSite: SymbolArg = DEFAULT_WEBSITE_CLS,
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
        self.Stream = symbol_by_name(Stream)
        self.Table = symbol_by_name(Table)
        self.WebSite = symbol_by_name(WebSite)
        self.store = store
        self._streams = OrderedDict()
        self._tables = OrderedDict()
        self._serializer_override = {}
        self._sensors = set()
        self._task_factories = []

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
        """Send event to stream soon.

        This is for use by non-async functions.
        """
        self._message_buffer.put((topic, key, value))

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
        """Deserialize message key.

        Arguments:
            typ: Model to use for deserialization.
            key: Serialized key.
        """
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
        """Deserialize message value.

        Arguments:
            typ: Model to use for deserialization.
            key: Deserialized key.
            message: Message instance containing the serialized message body.
        """
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
        """Serialize key.

        Arguments:
            topic: The topic that the message will be sent to.
            key: The key to be serialized.
        """
        if isinstance(key, ModelT):
            try:
                ser = self._get_serializer(key._options.serializer)
            except KeyError:
                return key.dumps()
            else:
                return await ser.dumps_key(topic, key)
        return want_bytes(key) if key is not None else key

    async def dumps_value(self, topic: str, value: V) -> Optional[bytes]:
        """Serialize value.

        Arguments:
            topic: The topic that the message will be sent to.
            value: The value to be serialized.
        """
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

    def task(self, task: Callable[[AppT], Generator]) -> Callable:
        self._task_factories.append(task)
        return task

    def add_task(self, task: Generator) -> Awaitable:
        """Start task.

        Notes:
            A task is simply any coroutine iterating over a stream,
            or even any coroutine doing anything.
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
            topic: Topic description to consume from.

        Keyword Arguments:
            coroutine: Coroutine to filter events in this stream.
            processors: List of processors for events in
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
              window: WindowT = None,
              **kwargs: Any) -> TableT:
        """Create new table.

        Arguments:
            table_name: Name used for table, note that two tables living in
            the same application cannot have the same name.

        Keyword Arguments:
            default: A callable, or type that will return a default value
            for keys missing in this table.
            window: A windowing strategy for this table. This windowing
            strategy is used to clean stale keys from the table.

        Examples:
            >>> table = app.table('user_to_amount', default=int)
            >>> table['George']
            0
            >>> table['Elaine'] += 1
            >>> table['Elaine'] += 1
            >>> table['Elaine']
            2
        """
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
        """Attach new sensor to this application."""
        # connect beacons
        sensor.beacon = self.beacon.new(sensor)
        self._sensors.add(sensor)

    def remove_sensor(self, sensor: SensorT) -> None:
        """Detach sensor from this application."""
        self._sensors.remove(sensor)

    async def on_message_in(
            self,
            consumer_id: int,
            offset: int,
            message: Message) -> None:
        """Called whenever a message is received.

        Delegates to sensors, so instead of redefining this method
        you should add a sensor.
        """
        for _sensor in self._sensors:
            await _sensor.on_message_in(consumer_id, offset, message)

    async def on_message_out(
            self,
            consumer_id: int,
            offset: int,
            message: Message = None) -> None:
        """Called whenever a message has finished processing.

        Delegates to sensors, so instead of redefining this method
        you should add a sensor.
        """
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
        return self._new_name(_constants.SOURCE_NAME)

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

    def render_graph(self) -> str:
        """Render graph of application components to DOT format."""
        o = io.StringIO()
        beacon = self.beacon.root if self.beacon.root else self.beacon
        beacon.as_graph().to_dot(o)
        return o.getvalue()

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
        """Default producer instance."""
        if self._producer is None:
            self._producer = self._new_producer()
        return self._producer

    @producer.setter
    def producer(self, producer: ProducerT) -> None:
        self._producer = producer

    @property
    def transport(self) -> TransportT:
        """Message transport."""
        if self._transport is None:
            self._transport = self._create_transport()
        return self._transport

    @transport.setter
    def transport(self, transport: TransportT) -> None:
        self._transport = transport

    @property
    def tasks_running(self) -> int:
        """Number of active tasks."""
        return len(self._tasks)

    @cached_property
    def _service(self) -> ServiceT:
        return AppService(self)

    @cached_property
    def website(self) -> Web:
        return self.WebSite(self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def streams(self) -> StreamManagerT:
        return StreamManager(app=self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def _message_buffer(self) -> asyncio.Queue:
        return asyncio.Queue(loop=self.loop)
