"""Faust Application."""
import asyncio
import faust

from collections import defaultdict
from functools import wraps
from heapq import heappush, heappop
from itertools import chain
from typing import (
    Any, AsyncIterable, AsyncIterator, Awaitable, Callable,
    Iterable, Iterator, List, MutableMapping, MutableSequence,
    Optional, Pattern, Sequence, Union, Type, Tuple, cast,
)

from . import transport
from .actors import ActorFun, Actor, ActorT
from .exceptions import ImproperlyConfigured
from .sensors import SensorDelegate
from .topics import Topic, TopicManager, TopicManagerT
from .types import (
    CodecArg, K, Message, PendingMessage,
    StreamCoroutine, TopicT, TopicPartition, V,
)
from .types.app import AppT
from .sensors import Monitor
from .types.streams import StreamT
from .types.tables import TableT
from .types.transports import ConsumerT, ProducerT, TPorTopicSet, TransportT
from .types.windows import WindowT
from .utils.aiter import aiter
from .utils.compat import OrderedDict
from .utils.imports import SymbolArg, symbol_by_name
from .utils.logging import get_logger
from .utils.objects import cached_property
from .utils.services import Service, ServiceProxy, ServiceT
from .utils.times import Seconds, want_seconds
from .utils.types.collections import NodeT
from .web import Web

__all__ = ['App']

__flake8_please_Any_is_OK: Any   # flake8 thinks Any is unused :/
__flake8_please_AsyncIterator_is_OK: AsyncIterator

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
CLIENT_ID = f'faust-{faust.__version__}'

#: How often we commit messages.
#: Can be customized by setting ``App(commit_interval=...)``.
COMMIT_INTERVAL = 30.0

#: Format string for ``repr(app)``.
APP_REPR = """
<{name}({s.id}): {s.url} {s.state} actors({actors}) sources({sources})>
""".strip()

logger = get_logger(__name__)


class AppService(Service):
    """Service responsible for starting/stopping an application."""

    # App is created in module scope so we split it up to ensure
    # Service.loop does not create the asyncio event loop
    # when a module is imported.

    def __init__(self, app: 'App', **kwargs: Any) -> None:
        self.app: App = app
        super().__init__(loop=self.app.loop, **kwargs)

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        # Add all asyncio.Tasks, like timers, etc.
        for task in self.app._tasks:
            self.add_future(task())

        # Add the main Monitor sensor.
        self.app.sensors.add(self.app.monitor)

        # Then return the list of "subservices",
        # those that'll be started when the app starts,
        # stopped when the app stops,
        # etc...
        return cast(Iterable[ServiceT], chain(
            # Sensors must always be started first, and stopped last.
            self.app.sensors,
            # Producer must be stoppped after consumer.
            [self.app.producer],                      # app.Producer
            # Consumer must be stopped after Topic Manager
            [self.app.consumer],                      # app.Consumer
            # Tables (and Sets).
            self.app.tables.values(),
            # WebSite
            [self.app.website],                       # app.WebSite
            # TopicManager stops consumer
            [self.app.sources],                       # app.TopicManager
            # Actors last.
            self.app.actors.values(),
        ))

    async def on_first_start(self) -> None:
        if not self.app.actors:
            raise ImproperlyConfigured(
                'Attempting to start app that has no actors')

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
        await self._stopped.wait()

    async def _drain_message_buffer(self) -> None:
        send = self.app.send
        get = self.app._message_buffer.get
        while not self.should_stop:
            pending = await get()
            send(
                pending.topic, pending.key, pending.value,
                partition=pending.partition,
                key_serializer=pending.key_serializer,
                value_serializer=pending.value_serializer,
            )

    @property
    def label(self) -> str:
        return f'{type(self).__name__}: {self.app.id}@{self.app.url}'


class App(AppT, ServiceProxy):
    """Faust Application.

    Arguments:
        id (str): Application ID.

    Keyword Arguments:
        url (str):
            Transport URL.  Default: ``"aiokafka://localhost:9092"``.
        client_id (str):  Client id used for producer/consumer.
        commit_interval (Seconds): How often we commit messages that
            have been fully processed.  Default ``30.0``.
        key_serializer (CodecArg): Default serializer for Topics
            that do not have an explicit serializer set.
            Default: :const:`None`.
        value_serializer (CodecArg): Default serializer for event types
            that do not have an explicit serializer set.  Default: ``"json"``.
        num_standby_replicas (int): The number of standby replicas for each
            table.  Default: ``0``.
        replication_factor (int): The replication factor for changelog topics
            and repartition topics created by the application.  Default: ``1``.
        loop (asyncio.AbstractEventLoop):
            Provide specific asyncio event loop instance.
    """

    #: Default producer instance.
    _producer: Optional[ProducerT] = None

    #: Set when producer is started.
    _producer_started: bool = False

    #: Default consumer instance.
    _consumer: Optional[ConsumerT] = None

    #: Set when consumer is started.
    _consumer_started: bool = False

    #: Transport is created on demand: use `.transport`.
    _transport: Optional[TransportT] = None

    _pending_on_commit: MutableMapping[
        TopicPartition,
        List[Tuple[int, PendingMessage]]]

    _monitor: Monitor = None

    _tasks: MutableSequence[Callable[[], Awaitable]]

    def start(self, *,
              argv: Sequence[str] = None,
              loop: asyncio.AbstractEventLoop = None) -> None:
        from .bin.base import parse_worker_args
        from .worker import Worker
        from .sensors import Monitor
        self.sensors.add(Monitor())
        kwargs = parse_worker_args(argv, standalone_mode=False)
        Worker(self, loop=loop, **kwargs).execute_from_commandline()

    def __init__(self, id: str,
                 *,
                 url: str = 'aiokafka://localhost:9092',
                 store: str = 'memory://',
                 avro_registry_url: str = None,
                 client_id: str = CLIENT_ID,
                 commit_interval: Seconds = COMMIT_INTERVAL,
                 key_serializer: CodecArg = 'json',
                 value_serializer: CodecArg = 'json',
                 num_standby_replicas: int = 0,
                 replication_factor: int = 1,
                 Stream: SymbolArg = DEFAULT_STREAM_CLS,
                 Table: SymbolArg = DEFAULT_TABLE_CLS,
                 WebSite: SymbolArg = DEFAULT_WEBSITE_CLS,
                 Serializers: SymbolArg = DEFAULT_SERIALIZERS_CLS,
                 monitor: Monitor = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop
        self.id = id
        self.url = url
        self.client_id = client_id
        self.commit_interval = want_seconds(commit_interval)
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
        self.actors = OrderedDict()
        self.tables = OrderedDict()
        self.sensors = SensorDelegate(self)
        self.store = store
        self._monitor = monitor
        self._tasks = []
        self._pending_on_commit = defaultdict(list)

    def topic(self, *topics: str,
              pattern: Union[str, Pattern] = None,
              key_type: Type = None,
              value_type: Type = None) -> TopicT:
        return Topic(
            self,
            topics=topics,
            pattern=pattern,
            key_type=key_type,
            value_type=value_type,
        )

    def actor(self, topic: TopicT,
              *,
              name: str = None,
              concurrency: int = 1) -> Callable[[ActorFun], ActorT]:
        def _inner(fun: ActorFun) -> ActorT:
            actor = Actor(
                fun,
                name=name,
                app=self,
                topic=topic,
                concurrency=concurrency,
                on_error=self._on_actor_error,
            )
            self.actors[actor.name] = actor
            return actor
        return _inner

    def task(self, fun: Callable[[], Awaitable]) -> Callable:
        self._tasks.append(fun)
        return fun

    def timer(self, interval: Seconds) -> Callable:
        interval_s = want_seconds(interval)

        def _inner(fun: Callable[[AppT], Awaitable]) -> Callable:
            @self.task
            @wraps(fun)
            async def around_timer(app: AppT) -> None:
                while not app.should_stop:
                    await asyncio.sleep(interval_s, loop=app.loop)
                    await fun(app)
            return around_timer
        return _inner

    def stream(self, source: Union[AsyncIterable, Iterable],
               coroutine: StreamCoroutine = None,
               **kwargs: Any) -> StreamT:
        """Create new stream from topic.

        Arguments:
            source: Async iterable to stream over.

        Keyword Arguments:
            coroutine: Coroutine to filter events in this stream.
            kwargs: See :class:`Stream`.

        Returns:
            faust.Stream:
                to iterate over events in the stream.
        """
        return self.Stream(
            source=aiter(source) if source is not None else None,
            coroutine=coroutine,
            beacon=self.beacon,
            **kwargs)

    def table(self, table_name: str,
              *,
              default: Callable[[], Any] = None,
              window: WindowT = None,
              **kwargs: Any) -> TableT:
        """Create new table.

        Arguments:
            table_name: Name used for table, note that two tables living in
            the same application cannot have the same name.

        Keyword Arguments:
            default: A callable, or type that will return a default value
            for keys missing in this table.
            window: A windowing strategy to wrap this window in.

        Examples:
            >>> table = app.table('user_to_amount', default=int)
            >>> table['George']
            0
            >>> table['Elaine'] += 1
            >>> table['Elaine'] += 1
            >>> table['Elaine']
            2
        """
        table = self.Table(
            self,
            table_name=table_name,
            default=default,
            beacon=self.beacon,
            **kwargs)
        self.add_table(table)
        return table.using_window(window) if window else table

    def add_table(self, table: TableT) -> None:
        """Register existing table."""
        assert table.table_name
        if table.table_name in self.tables:
            raise ValueError(
                f'Table with name {table.table_name!r} already exists')
        self.tables[table.table_name] = table

    async def send(
            self,
            topic: Union[TopicT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            *,
            wait: bool = True) -> Awaitable:
        """Send event to stream.

        Arguments:
            topic (Union[TopicT, str]): Topic to send event to.
            key (K): Message key.
            value (V): Message value.
            partition (int): Specific partition to send to.
                If not set the partition will be chosen by the partitioner.
            key_serializer (CodecArg): Serializer to use
                only when key is not a model.
            value_serializer (CodecArg): Serializer to use
                only when key is not a model.

        Keyword Arguments:
            wait (bool): Wait for message to be published (default),
                if unset the message will only be appended to the buffer.
        """
        strtopic: str
        if isinstance(topic, TopicT):
            # ridiculous casting
            topictopic = cast(TopicT, topic)
            strtopic = topictopic.topics[0]
        else:
            strtopic = cast(str, topic)
        return await self._send(
            strtopic,
            (await self.serializers.dumps_key(
                strtopic, key, key_serializer)
             if key is not None else None),
            (await self.serializers.dumps_value(
                strtopic, value, value_serializer)),
            wait=wait,
            partition=partition,
        )

    async def send_many(
            self, it: Iterable[Union[PendingMessage, Tuple]]) -> None:
        await asyncio.wait(
            [self._send_tuple(msg) for msg in it],
            loop=self.loop,
            return_when=asyncio.ALL_COMPLETED,
        )

    async def _send_tuple(
            self, message: Union[PendingMessage, Tuple],
            wait: bool = True) -> Awaitable:
        return await self.send(
            *self._unpack_message_tuple(*message), wait=wait)

    def _unpack_message_tuple(
            self,
            topic: str,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None) -> PendingMessage:
        return PendingMessage(
            topic, key, value, partition,
            key_serializer, value_serializer)

    def send_soon(self, topic: Union[TopicT, str], key: K, value: V,
                  partition: int = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None) -> None:
        """Send event to stream soon.

        This is for use by non-async functions.
        """
        self._message_buffer.put(PendingMessage(
            topic, key, value, partition,
            key_serializer, value_serializer,
        ))

    def send_attached(self,
                      message: Message,
                      topic: Union[str, TopicT],
                      key: K,
                      value: V,
                      partition: int = None,
                      key_serializer: CodecArg = None,
                      value_serializer: CodecArg = None) -> None:
        buffer = self._pending_on_commit[message.tp]
        pending_message = PendingMessage(
            topic, key, value, partition,
            key_serializer, value_serializer)
        heappush(buffer, (message.offset, pending_message))

    async def commit_attached(self, tp: TopicPartition, offset: int) -> None:
        # Get pending messages attached to this TP+offset
        attached = list(self._get_attached(tp, offset))
        if attached:
            # Send the messages in one go.
            await self.send_many(attached)

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
                    partition: int = None,
                    key_serializer: CodecArg = None,
                    value_serializer: CodecArg = None,
                    *,
                    wait: bool = True) -> Awaitable:
        logger.debug('send: topic=%r key=%r value=%r', topic, key, value)
        producer = self.producer
        if not self._producer_started:
            self._producer_started = True
            # producer may also have been started by app.start()
            await producer.maybe_start()
        if wait:
            state = await self.sensors.on_send_initiated(
                producer, topic,
                keysize=len(key) if key else 0,
                valsize=len(value) if value else 0)
            ret = await producer.send_and_wait(
                topic, key, value, partition=partition)
            await self.sensors.on_send_completed(producer, state)
            return ret
        return producer.send(topic, key, value, partition=partition)

    async def _on_actor_error(
            self, actor: ActorT, exc: Exception) -> None:
        if self.sources.consumer:
            try:
                await self.sources.consumer.on_task_error(exc)
            except Exception as exc:
                logger.exception('Consumer error callback raised: %r', exc)

    async def commit(self, topics: TPorTopicSet) -> bool:
        return await self.sources.commit(topics)

    def _new_producer(self, beacon: NodeT = None) -> ProducerT:
        return self.transport.create_producer(
            beacon=beacon or self.beacon,
        )

    def _new_consumer(self) -> ConsumerT:
        return self.transport.create_consumer(
            callback=self.sources.on_message,
            on_partitions_revoked=self.sources.on_partitions_revoked,
            on_partitions_assigned=self.sources.on_partitions_assigned,
            beacon=self.beacon,
        )

    def _create_transport(self) -> TransportT:
        return cast(TransportT,
                    transport.by_url(self.url)(self.url, self, loop=self.loop))

    def __repr__(self) -> str:
        return APP_REPR.format(
            name=type(self).__name__,
            s=self,
            actors=self.actors,
            sources=len(self.sources),
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
    def consumer(self) -> ConsumerT:
        """Default consumer instance."""
        if self._consumer is None:
            self._consumer = self._new_consumer()
        return self._consumer

    @consumer.setter
    def consumer(self, consumer: ConsumerT) -> None:
        self._consumer = consumer

    @property
    def transport(self) -> TransportT:
        """Message transport."""
        if self._transport is None:
            self._transport = self._create_transport()
        return self._transport

    @transport.setter
    def transport(self, transport: TransportT) -> None:
        self._transport = transport

    @cached_property
    def _service(self) -> ServiceT:
        return AppService(self)

    @cached_property
    def website(self) -> Web:
        return self.WebSite(self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def sources(self) -> TopicManagerT:
        return TopicManager(app=self, loop=self.loop, beacon=self.beacon)

    @property
    def monitor(self) -> Monitor:
        if self._monitor is None:
            self._monitor = Monitor()
        return self._monitor

    @monitor.setter
    def monitor(self, monitor: Monitor) -> None:
        self._monitor = monitor

    @cached_property
    def _message_buffer(self) -> asyncio.Queue:
        return asyncio.Queue(loop=self.loop)
