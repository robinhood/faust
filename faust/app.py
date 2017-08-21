"""Faust Application."""
import asyncio

from collections import defaultdict
from datetime import timedelta
from functools import wraps
from heapq import heappop, heappush
from itertools import chain
from pathlib import Path
from typing import (
    Any, AsyncIterable, Awaitable, Callable,
    Iterable, Iterator, List, Mapping, MutableMapping, MutableSequence,
    Optional, Pattern, Sequence, Tuple, Type, Union, cast,
)
from uuid import uuid4

from . import __version__ as faust_version
from . import transport
from .actors import Actor, ActorFun, ActorT, ReplyConsumer, SinkT
from .assignor import PartitionAssignor
from .exceptions import ImproperlyConfigured
from .sensors import Monitor, SensorDelegate
from .streams import current_event
from .topics import Topic, TopicManager, TopicManagerT
from .types import (
    CodecArg, FutureMessage, K, Message, MessageSentCallback, ModelArg,
    PendingMessage, RecordMetadata, StreamCoroutine, TopicPartition, TopicT, V,
)
from .types.app import AppT
from .types.serializers import RegistryT
from .types.streams import StreamT
from .types.tables import CollectionT, SetT, TableManagerT, TableT
from .types.transports import (
    ConsumerT, ProducerT,  TPorTopic, TPorTopicSet, TransportT,
)
from .types.windows import WindowT
from .utils.aiter import aiter
from .utils.compat import OrderedDict
from .utils.futures import maybe_async
from .utils.imports import SymbolArg, symbol_by_name
from .utils.logging import get_logger
from .utils.objects import Unordered, cached_property
from .utils.services import Service, ServiceProxy, ServiceT
from .utils.times import Seconds, want_seconds
from .utils.types.collections import NodeT

__all__ = ['App']

#: Default broker URL.
DEFAULT_URL = 'kafka://localhost:9092'

#: Path to default stream class used by ``app.stream``.
DEFAULT_STREAM_CLS = 'faust.Stream'

DEFAULT_TABLE_MAN = 'faust.tables.TableManager'

#: Path to default table class used by ``app.Table``.
DEFAULT_TABLE_CLS = 'faust.Table'

#: Path to default set class used by ``app.Set``.
DEFAULT_SET_CLS = 'faust.Set'

#: Path to default serializer registry class.
DEFAULT_SERIALIZERS_CLS = 'faust.serializers.Registry'

#: Path to keep table changelog cache.  If None (default) the current
#: directory is used.
DEFAULT_TABLE_CACHE_PATH = None

#: Default Kafka Client ID.
CLIENT_ID = f'faust-{faust_version}'

#: How often we commit messages.
#: Can be customized by setting ``App(commit_interval=...)``.
COMMIT_INTERVAL = 2.0

#: How often we clean up windowed tables.
#: Can be customized by setting ``App(table_cleanup_interval=...)``.
TABLE_CLEANUP_INTERVAL = 30.0

#: Prefix used for reply topics.
REPLY_TOPIC_PREFIX = 'f-reply-'

#: Default expiry time for replies in seconds (float/timedelta).
DEFAULT_REPLY_EXPIRES = timedelta(days=1)

#: Format string for ``repr(app)``.
APP_REPR = """
<{name}({s.id}): {s.url} {s.state} actors({actors}) channels({channels})>
""".strip()

logger = get_logger(__name__)

_flake8_RecordMetadata_is_used: RecordMetadata  # XXX flake8 bug


class AppService(Service):
    """Service responsible for starting/stopping an application."""
    logger = logger

    # App is created in module scope so we split it up to ensure
    # Service.loop does not create the asyncio event loop
    # when a module is imported.

    def __init__(self, app: 'App', **kwargs: Any) -> None:
        self.app: App = app
        super().__init__(loop=self.app.loop, **kwargs)

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        if self.app.client_only:
            return self._components_client()
        return self._components_server()

    def _components_client(self) -> Iterable[ServiceT]:
        return cast(Iterable[ServiceT], chain(
            [self.app.producer],
            [self.app.consumer],
            [self.app._reply_consumer],
            [self.app.channels],
            [self.app._fetcher],
        ))

    def _components_server(self) -> Iterable[ServiceT]:
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
            # ReplyConsumer
            [self.app._reply_consumer],
            # Actors
            self.app.actors.values(),
            # TopicManager
            [self.app.channels],                      # app.TopicManager
            # TableManager
            [self.app.tables],                        # app.TableManager
            # Fetcher
            [self.app._fetcher],
        ))

    async def on_first_start(self) -> None:
        if not self.app.actors:
            raise ImproperlyConfigured(
                'Attempting to start app that has no actors')

    async def on_started(self) -> None:
        if self.app.on_startup_finished:
            await self.app.on_startup_finished()

    @Service.task
    async def _drain_message_buffer(self) -> None:
        send = self.app.send
        get = self.app._message_buffer.get
        while not self.should_stop:
            fut: FutureMessage = await get()
            pending: PendingMessage = fut.message
            send(
                pending.topic, pending.key, pending.value,
                partition=pending.partition,
                key_serializer=pending.key_serializer,
                value_serializer=pending.value_serializer,
            )

    @property
    def label(self) -> str:
        return self.app.label

    @property
    def shortlabel(self) -> str:
        return self.app.shortlabel


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
        table_cache_path (Union[str, pathlib.Path]): Path to store cached table
            changelog keys.
        loop (asyncio.AbstractEventLoop):
            Provide specific asyncio event loop instance.
    """
    logger = logger

    client_only = False

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
        List[Tuple[int, Unordered[FutureMessage]]]]

    _monitor: Monitor = None

    _tasks: MutableSequence[Callable[[], Awaitable]]

    def start_worker(self, *,
                     argv: Sequence[str] = None,
                     loop: asyncio.AbstractEventLoop = None) -> None:
        from .bin.base import parse_worker_args
        from .worker import Worker
        kwargs = parse_worker_args(argv, standalone_mode=False)
        Worker(self, loop=loop, **kwargs).execute_from_commandline()

    def __init__(
            self, id: str,
            *,
            url: str = 'aiokafka://localhost:9092',
            store: str = 'memory://',
            avro_registry_url: str = None,
            client_id: str = CLIENT_ID,
            commit_interval: Seconds = COMMIT_INTERVAL,
            table_cleanup_interval: Seconds = TABLE_CLEANUP_INTERVAL,
            key_serializer: CodecArg = 'json',
            value_serializer: CodecArg = 'json',
            num_standby_replicas: int = 0,
            replication_factor: int = 1,
            default_partitions: int = 8,
            reply_to: str = None,
            create_reply_topic: bool = False,
            table_cache_path: Union[Path, str] = DEFAULT_TABLE_CACHE_PATH,
            reply_expires: Seconds = DEFAULT_REPLY_EXPIRES,
            Stream: SymbolArg[Type[StreamT]] = DEFAULT_STREAM_CLS,
            Table: SymbolArg[Type[TableT]] = DEFAULT_TABLE_CLS,
            TableManager: SymbolArg[Type[TableManagerT]] = DEFAULT_TABLE_MAN,
            Set: SymbolArg[Type[SetT]] = DEFAULT_SET_CLS,
            Serializers: SymbolArg[Type[RegistryT]] = DEFAULT_SERIALIZERS_CLS,
            monitor: Monitor = None,
            on_startup_finished: Callable = None,
            loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop
        self.id = id
        self.url = url
        self.client_id = client_id
        self.commit_interval = want_seconds(commit_interval)
        self.table_cleanup_interval = want_seconds(table_cleanup_interval)
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.num_standby_replicas = num_standby_replicas
        self.replication_factor = replication_factor
        self.default_partitions = default_partitions
        self.reply_to = reply_to or REPLY_TOPIC_PREFIX + str(uuid4())
        self.create_reply_topic = create_reply_topic
        self.table_cache_path = Path(table_cache_path or Path.cwd())
        self.reply_expires = want_seconds(
            reply_expires or DEFAULT_REPLY_EXPIRES)
        self.avro_registry_url = avro_registry_url
        self.Stream = symbol_by_name(Stream)
        self.TableType = symbol_by_name(Table)
        self.SetType = symbol_by_name(Set)
        self.TableManager = symbol_by_name(TableManager)
        self.Serializers = symbol_by_name(Serializers)
        self.serializers = self.Serializers(
            key_serializer=self.key_serializer,
            value_serializer=self.value_serializer,
        )
        self.assignor = PartitionAssignor(self,
                                          replicas=self.replication_factor)
        self.actors = OrderedDict()
        self.sensors = SensorDelegate(self)
        self.store = store
        self._monitor = monitor
        self._tasks = []
        self._pending_on_commit = defaultdict(list)
        self.on_startup_finished: Callable = on_startup_finished
        ServiceProxy.__init__(self)

    def topic(self, *topics: str,
              pattern: Union[str, Pattern] = None,
              key_type: ModelArg = None,
              value_type: ModelArg = None,
              partitions: int = None,
              retention: Seconds = None,
              compacting: bool = None,
              deleting: bool = None,
              replicas: int = None,
              config: Mapping[str, Any] = None) -> TopicT:
        return Topic(
            self,
            topics=topics,
            pattern=pattern,
            key_type=key_type,
            value_type=value_type,
            partitions=partitions,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
            config=config,
        )

    def actor(self,
              topic: Union[str, TopicT] = None,
              *,
              name: str = None,
              concurrency: int = 1,
              sink: Iterable[SinkT] = None) -> Callable[[ActorFun], ActorT]:
        def _inner(fun: ActorFun) -> ActorT:
            actor = Actor(
                fun,
                name=name,
                app=self,
                topic=topic,
                concurrency=concurrency,
                sink=sink,
                on_error=self._on_actor_error,
            )
            self.actors[actor.name] = actor
            return actor
        return _inner

    async def _on_actor_error(
            self, actor: ActorT, exc: Exception) -> None:
        if self._consumer:
            try:
                await self._consumer.on_task_error(exc)
            except Exception as exc:
                self.log.exception('Consumer error callback raised: %r', exc)

    def task(self, fun: Callable[[], Awaitable]) -> Callable:
        self._tasks.append(fun)
        return fun

    def timer(self, interval: Seconds) -> Callable:
        interval_s = want_seconds(interval)

        def _inner(fun: Callable[..., Awaitable]) -> Callable:
            @self.task
            @wraps(fun)
            async def around_timer(*args: Any, **kwargs: Any) -> None:
                while not self._service.should_stop:
                    await self._service.sleep(interval_s)
                    await fun(*args, **kwargs)
            return around_timer
        return _inner

    def stream(self, channel: Union[AsyncIterable, Iterable],
               coroutine: StreamCoroutine = None,
               beacon: NodeT = None,
               **kwargs: Any) -> StreamT:
        """Create new stream from topic.

        Arguments:
            channel: Async iterable to stream over.

        Keyword Arguments:
            coroutine: Coroutine to filter events in this stream.
            kwargs: See :class:`Stream`.

        Returns:
            faust.Stream:
                to iterate over events in the stream.
        """
        return self.Stream(
            channel=aiter(channel) if channel is not None else None,
            coroutine=coroutine,
            beacon=beacon or self.beacon,
            **kwargs)

    def Table(self, name: str,
              *,
              default: Callable[[], Any] = None,
              window: WindowT = None,
              partitions: int = None,
              **kwargs: Any) -> TableT:
        """Create new table.

        Arguments:
            name: Name used for table, note that two tables living in
                the same application cannot have the same name.

        Keyword Arguments:
            default: A callable, or type that will return a default value
            for keys missing in this table.
            window: A windowing strategy to wrap this window in.

        Examples:
            >>> table = app.Table('user_to_amount', default=int)
            >>> table['George']
            0
            >>> table['Elaine'] += 1
            >>> table['Elaine'] += 1
            >>> table['Elaine']
            2
        """
        table = self.TableType(
            self,
            name=name,
            default=default,
            beacon=self.beacon,
            partitions=partitions,
            **kwargs)
        self.add_collection(table)
        return table.using_window(window) if window else table

    def Set(self, name: str,
            *,
            window: WindowT = None,
            partitions: int = None,
            **kwargs: Any) -> SetT:
        set_ = self.SetType(
            self,
            name=name,
            beacon=self.beacon,
            partitions=partitions,
            window=window,
            **kwargs,
        )
        self.add_collection(set_)
        return set_

    def add_collection(self, table: CollectionT) -> None:
        """Register existing table."""
        assert table.name
        if table.name in self.tables:
            raise ValueError(
                f'Table with name {table.name!r} already exists')
        self.tables[table.name] = table

    async def start_client(self) -> None:
        self.client_only = True
        await self._service.maybe_start()

    async def maybe_start_client(self) -> None:
        if not self._service.started:
            await self.start_client()

    async def maybe_attach(
            self,
            topic: Union[TopicT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None,
            force: bool = False) -> FutureMessage:
        if not force:
            event = current_event()
            if event is not None:
                return event.attach(
                    topic, key, value,
                    partition=partition,
                    key_serializer=key_serializer,
                    value_serializer=value_serializer,
                    callback=callback,
                )
        return await self.send(
            topic, key, value,
            partition=partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

    async def send(
            self,
            topic: Union[TopicT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> FutureMessage:
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
                only when value is not a model.
        """
        strtopic: str
        if isinstance(topic, TopicT):
            # ridiculous casting
            topictopic = cast(TopicT, topic)
            strtopic = topictopic.topics[0]
        else:
            strtopic = cast(str, topic)

        fut = FutureMessage(PendingMessage(
            strtopic,
            (await self.serializers.dumps_key(
                strtopic, key, key_serializer)
             if key is not None else None),
            (await self.serializers.dumps_value(
                strtopic, value, value_serializer)),
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            partition=partition,
            callback=callback,
        ))
        return await self._send_future(fut)

    async def send_many(
            self, it: Iterable[Union[PendingMessage, Tuple]]) -> None:
        """Send a list of messages (unordered)."""
        await asyncio.wait(
            [self._send_tuple(msg) for msg in it],
            loop=self.loop,
            return_when=asyncio.ALL_COMPLETED,
        )

    async def _send_tuple(
            self, message: Union[PendingMessage, Tuple]) -> FutureMessage:
        return await self.send(*self._unpack_message_tuple(*message))

    def _unpack_message_tuple(
            self,
            topic: str,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> PendingMessage:
        return PendingMessage(
            topic, key, value, partition,
            key_serializer, value_serializer, callback)

    def send_soon(self, topic: Union[TopicT, str], key: K, value: V,
                  partition: int = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None,
                  callback: MessageSentCallback = None) -> FutureMessage:
        """Send event to stream soon.

        This is for use by non-async functions.
        """
        fut = FutureMessage(PendingMessage(
            topic, key, value, partition,
            key_serializer, value_serializer, callback,
        ))
        self._message_buffer.put(fut)
        return fut

    def send_attached(self,
                      message: Message,
                      topic: Union[str, TopicT],
                      key: K,
                      value: V,
                      partition: int = None,
                      key_serializer: CodecArg = None,
                      value_serializer: CodecArg = None,
                      callback: MessageSentCallback = None) -> FutureMessage:
        buf = self._pending_on_commit[message.tp]
        fut = FutureMessage(PendingMessage(
            topic, key, value, partition,
            key_serializer, value_serializer, callback))
        heappush(buf, (message.offset, Unordered(fut)))
        return fut

    async def commit_attached(self, tp: TopicPartition, offset: int) -> None:
        # publish pending messages attached to this TP+offset
        for fut in list(self._get_attached(tp, offset)):
            await self._send_tuple(fut.message)

    def _get_attached(
            self,
            tp: TopicPartition,
            commit_offset: int) -> Iterator[FutureMessage]:
        attached = self._pending_on_commit.get(tp)
        while attached:
            # get the entry with the smallest offset in this TP
            entry = heappop(attached)

            # if the entry offset is smaller or equal to the offset
            # being committed
            if entry[0] <= commit_offset:
                # we use it
                yield entry[1].value  # Only yield FutureMessage (not offset)
            else:
                # we put it back and exit, as this was the smallest offset.
                heappush(attached, entry)
                break

    async def _send_future(self, fut: FutureMessage) -> FutureMessage:
        message: PendingMessage = fut.message
        topic: str = cast(str, message.topic)
        key: bytes = cast(bytes, message.key)
        value: bytes = cast(bytes, message.value)
        self.log.debug('send: topic=%r key=%r value=%r', topic, key, value)
        assert topic is not None
        producer = await self.maybe_start_producer()
        state = await self.sensors.on_send_initiated(
            producer, topic,
            keysize=len(key) if key else 0,
            valsize=len(value) if value else 0)
        ret: RecordMetadata = await producer.send_and_wait(
            topic, key, value, partition=message.partition)
        fut.set_result(ret)
        await self.sensors.on_send_completed(producer, state)
        if message.callback:
            await maybe_async(message.callback(fut))
        return fut

    async def maybe_start_producer(self) -> ProducerT:
        producer = self.producer
        if not self._producer_started:
            self._producer_started = True
            # producer may also have been started by app.start()
            await producer.maybe_start()
        return producer

    def _should_commit(self, topic: TPorTopic) -> bool:
        t = topic.topic if isinstance(topic, TopicPartition) else topic
        # We don't commit changelog tps that are resumed (standbys)
        return t in self.tables.changelog_topics

    async def commit(self, topics: TPorTopicSet) -> bool:
        commit_topics = filter(self._should_commit, topics)
        print('Committing topics', commit_topics)
        return await self.channels.commit(commit_topics)

    def _new_producer(self, beacon: NodeT = None) -> ProducerT:
        return self.transport.create_producer(
            beacon=beacon or self.beacon,
        )

    def _new_consumer(self) -> ConsumerT:
        return self.transport.create_consumer(
            callback=self.channels.on_message,
            on_partitions_revoked=self.on_partitions_revoked,
            on_partitions_assigned=self.on_partitions_assigned,
            beacon=self.beacon,
        )

    async def on_partitions_assigned(
            self, assigned: Iterable[TopicPartition]) -> None:
        await self.channels.on_partitions_assigned(assigned)
        await self.tables.on_partitions_assigned(assigned)

    async def on_partitions_revoked(
            self, revoked: Iterable[TopicPartition]) -> None:
        self.log.dev('ON PARTITIONS REVOKED')
        await self.channels.on_partitions_revoked(revoked)
        assignment = self.consumer.assignment()
        if assignment:
            await self.consumer.pause_partitions(assignment)
            await self.consumer.wait_empty()
        else:
            self.log.dev('ON P. REVOKED NOT COMMITTING: ASSIGNMENT EMPTY')

    def _create_transport(self) -> TransportT:
        return transport.by_url(self.url)(self.url, self, loop=self.loop)

    def __repr__(self) -> str:
        return APP_REPR.format(
            name=type(self).__name__,
            s=self,
            actors=self.actors,
            channels=len(self.channels),
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
    def tables(self) -> TableManagerT:
        return self.TableManager(
            app=self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def channels(self) -> TopicManagerT:
        return TopicManager(app=self, loop=self.loop, beacon=self.beacon)

    @property
    def monitor(self) -> Monitor:
        if self._monitor is None:
            self._monitor = Monitor(loop=self.loop, beacon=self.beacon)
        return self._monitor

    @monitor.setter
    def monitor(self, monitor: Monitor) -> None:
        self._monitor = monitor

    @cached_property
    def _message_buffer(self) -> asyncio.Queue:
        return asyncio.Queue(loop=self.loop)

    @cached_property
    def _fetcher(self) -> ServiceT:
        return self.transport.Fetcher(self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def _reply_consumer(self) -> ReplyConsumer:
        return ReplyConsumer(self, loop=self.loop, beacon=self.beacon)

    @property
    def label(self) -> str:
        return f'{self.shortlabel}: {self.id}@{self.url}'

    @property
    def shortlabel(self) -> str:
        return type(self).__name__
