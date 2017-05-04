"""Faust Application."""
import asyncio
import faust
import io
import typing

from collections import OrderedDict
from functools import wraps
from heapq import heappush, heappop
from typing import (
    Any, Awaitable, Callable, ClassVar, Generator, Iterator, List,
    MutableMapping, MutableSequence, Optional, Sequence,
    Union, Tuple, cast,
)
from itertools import count
from weakref import WeakKeyDictionary

from . import transport
from .exceptions import ImproperlyConfigured
from .sensors import SensorDelegate
from .streams import _constants
from .streams.manager import StreamManager
from .types import (
    CodecArg, K, Message, PendingMessage,
    StreamCoroutine, Topic, TopicPartition, V,
)
from .types.app import AppT
from .types.streams import Processor, StreamT, StreamManagerT
from .types.tables import TableT
from .types.transports import ProducerT, TransportT
from .types.windows import WindowT
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

#: Path to default serializer registry class.
DEFAULT_SERIALIZERS_CLS = 'faust.serializers.Registry'

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
        for task, group_id in self.app._task_factories:
            self.app.add_task(task(self.app), group_id=group_id)
        # Stream+Table instances
        streams: List[ServiceT] = list(self.app._streams.values())
        sensors: List[ServiceT] = list(self.app.sensors)
        services: List[ServiceT] = [
            self.app.producer,                        # app.Producer
            self.app.website,                         # app.Web
            self.app.streams,                         # app.StreamManager
            self.app._tasks,                          # app.Group
        ]
        return cast(Sequence[ServiceT], sensors + streams + services)

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
        commit_interval (float): How often we commit messages that
            have been fully processed.  Default ``30.0``.
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

    _task_ids = count(0)

    _pending_on_commit: MutableMapping[
        TopicPartition,
        List[Tuple[int, PendingMessage]]]

    #: The @app.task decorator adds tasks to start here.
    #: for example:
    #:     app = faust.App('myid', tasks=[task])
    #:     >>> @app.task
    #:     def mytask(app):
    #:     ...     ...
    _task_factories: MutableSequence[
        Tuple[Callable[['AppT'], Generator], int]
    ]

    @classmethod
    def current_app(self) -> AppT:
        """Returns the app that created the active task."""
        return TASK_TO_APP[asyncio.Task.current_task(loop=self.loop)]

    def start(self, *,
              argv: Sequence[str] = None,
              loop: asyncio.AbstractEventLoop = None) -> None:
        from .bin.base import parse_worker_args
        from .worker import Worker
        from .sensors import Sensor
        self.sensors.add(Sensor())
        kwargs = parse_worker_args(argv, standalone_mode=False)
        Worker(self, loop=loop, **kwargs).execute_from_commandline()

    def __init__(self, id: str,
                 *,
                 url: str = 'aiokafka://localhost:9092',
                 store: str = 'memory://',
                 avro_registry_url: str = None,
                 client_id: str = CLIENT_ID,
                 commit_interval: float = COMMIT_INTERVAL,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = 'json',
                 num_standby_replicas: int = 0,
                 replication_factor: int = 1,
                 Stream: SymbolArg = DEFAULT_STREAM_CLS,
                 Table: SymbolArg = DEFAULT_TABLE_CLS,
                 WebSite: SymbolArg = DEFAULT_WEBSITE_CLS,
                 Serializers: SymbolArg = DEFAULT_SERIALIZERS_CLS,
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
        self.Serializers = symbol_by_name(Serializers)
        self.serializers = self.Serializers(
            key_serializer=self.key_serializer,
            value_serializer=self.value_serializer,
        )
        self.store = store
        self._streams = OrderedDict()
        self._tables = OrderedDict()
        self.sensors = SensorDelegate(self)
        self._task_factories = []
        self._pending_on_commit = {}

    async def send(
            self, topic: Union[Topic, str], key: K, value: V,
            *,
            wait: bool = True,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None) -> Awaitable:
        """Send event to stream.

        Arguments:
            topic (Union[Topic, str]): Topic to send event to.
            key (K): Message key.
            value (V): Message value.

        Keyword Arguments:
            wait (bool): Wait for message to be published (default),
                if unset the message will only be appended to the buffer.
            key_serializer (CodecArg): Serializer to use
                only when key is not a model.
            value_serializer (CodecArg): Serializer to use
                only when key is not a model.
        """
        if isinstance(topic, Topic):
            strtopic = topic.topics[0]
        return await self._send(
            strtopic,
            (await self.serializers.dumps_key(
                strtopic, key, key_serializer)
             if key is not None else None),
            (await self.serializers.dumps_value(
                strtopic, value, value_serializer)),
            wait=wait,
        )

    def send_soon(self, topic: Union[Topic, str], key: K, value: V,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None) -> None:
        """Send event to stream soon.

        This is for use by non-async functions.
        """
        self._message_buffer.put((
            topic, key, value,
            key_serializer, value_serializer,
        ))

    def send_attached(self,
                      message: Message,
                      topic: Union[str, Topic],
                      key: K,
                      value: V,
                      *,
                      key_serializer: CodecArg = None,
                      value_serializer: CodecArg = None) -> None:
        tp = TopicPartition(message.topic, message.partition)
        buffer = self._pending_on_commit[tp]
        heappush(
            buffer,
            (message.offset, PendingMessage(
                topic, key, value, key_serializer, value_serializer)),
        )

    def commit_attached(self, tp: TopicPartition, offset: int) -> None:
        # Get pending messages attached to this TP+offset
        attached = list(self._get_attached(tp, offset))
        if attached:
            # Send the messages, waiting for all of the writes
            # to complete by gathering the futures.
            asyncio.gather(*[
                self.send(topic, key, value,
                          key_serializer=keyser,
                          value_serializer=valser,
                          wait=False)
                for (topic, key, value, keyser, valser)
                in attached
            ])

    def _get_attached(
            self, tp: TopicPartition, commit_offset: int) -> Iterator:
        attached = self._pending_on_commit.get(tp)
        while attached:
            # get the entry with the smallest offset in this TP
            entry = heappop(attached)

            # if the entry offset is smaller or equal to the offset
            # being committed
            if entry[0] <= commit_offset:
                # we use it
                yield entry
            else:
                # we put it back and exit, as this was the smallest offset.
                heappush(attached, entry)
                break

    async def _send(self,
                    topic: str,
                    key: Optional[bytes],
                    value: Optional[bytes],
                    key_serializer: CodecArg = None,
                    value_serializer: CodecArg = None,
                    *,
                    wait: bool = True) -> Awaitable:
        logger.debug('send: topic=%r key=%r value=%r', topic, key, value)
        producer = self.producer
        if not self._producer_started:
            self._producer_started = True
            await producer.start()
        if wait:
            state = await self.sensors.on_send_initiated(
                producer, topic,
                keysize=len(key) if key else 0,
                valsize=len(value) if value else 0)
            ret = await producer.send_and_wait(topic, key, value)
            await self.sensors.on_send_completed(producer, state)
            return ret
        return producer.send(topic, key, value)

    def task(self, fun: Callable[[AppT], Generator] = None,
             *,
             concurrency: int = 1) -> None:
        # Support both `@task` and `@task(concurrency=1)`.
        if fun:
            return self._task(concurrency=concurrency)(fun)
        return self._task(concurrency=concurrency)

    def _task(self, *, concurrency: int = 1) -> Callable:
        def _inner(task: Callable[[AppT], Generator]) -> Callable:
            group_id = next(self._task_ids)
            self._task_factories.extend([(task, group_id)] * concurrency)
            return task
        return _inner

    def timer(self, interval: float) -> Callable:
        def _inner(task: Callable[[AppT], Awaitable]) -> Callable:
            @self.task
            @wraps(task)
            async def around_timer(app: AppT) -> None:
                while not app.should_stop:
                    await asyncio.sleep(interval)
                    await task(app)
            return around_timer
        return _inner

    def add_task(self, task: Generator, *, group_id: int = None) -> Awaitable:
        """Start task.

        Notes:
            A task is simply any coroutine iterating over a stream,
            or even any coroutine doing anything.
        """
        if group_id is None:
            group_id = next(self._task_ids)
        return self._tasks.add(task, group_id=group_id)

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
            window=window,
            **kwargs
        ).bind(self))

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
