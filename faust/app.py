"""Faust Application."""
import asyncio
import typing

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
from .channels import Channel, ChannelT
from .exceptions import ImproperlyConfigured
from .router import Router
from .sensors import Monitor, SensorDelegate
from .streams import current_event
from .topics import Topic, TopicManager, TopicManagerT
from .types import (
    CodecArg, FutureMessage, K, Message, MessageSentCallback, ModelArg,
    RecordMetadata, StreamCoroutine, TopicPartition, TopicT, V,
)
from .types.app import AppT
from .types.serializers import RegistryT
from .types.streams import StreamT
from .types.tables import (
    CheckpointManagerT, SetT, TableManagerT, TableT,
)
from .types.transports import (
    ConsumerT, ProducerT, TPorTopicSet, TransportT,
)
from .types.windows import WindowT
from .utils.aiter import aiter
from .utils.compat import OrderedDict
from .utils.futures import FlowControlEvent, FlowControlQueue, stampede
from .utils.imports import SymbolArg, symbol_by_name
from .utils.logging import get_logger
from .utils.objects import Unordered, cached_property
from .utils.services import Service, ServiceProxy, ServiceT
from .utils.times import Seconds, want_seconds
from .utils.types.collections import NodeT

if typing.TYPE_CHECKING:
    from .channels import Event
else:
    class Event: ...  # noqa

__all__ = ['App']

#: Default broker URL.
DEFAULT_URL = 'kafka://localhost:9092'

CHECKPOINT_PATH = '.checkpoint'

#: Path to default stream class used by ``app.stream``.
DEFAULT_STREAM_CLS = 'faust.Stream'

DEFAULT_TABLE_MAN = 'faust.tables.TableManager'

DEFAULT_CHECKPOINT_MAN = _DCPM = 'faust.tables.CheckpointManager'

#: Path to default table class used by ``app.Table``.
DEFAULT_TABLE_CLS = 'faust.Table'

#: Path to default set class used by ``app.Set``.
DEFAULT_SET_CLS = 'faust.Set'

#: Path to default serializer registry class.
DEFAULT_SERIALIZERS_CLS = 'faust.serializers.Registry'

#: Default Kafka Client ID.
CLIENT_ID = f'faust-{faust_version}'

#: How often we commit messages.
#: Can be customized by setting ``App(commit_interval=...)``.
COMMIT_INTERVAL = 3.0

#: How often we clean up windowed tables.
#: Can be customized by setting ``App(table_cleanup_interval=...)``.
TABLE_CLEANUP_INTERVAL = 30.0

#: Prefix used for reply topics.
REPLY_TOPIC_PREFIX = 'f-reply-'

#: Default expiry time for replies in seconds (float/timedelta).
DEFAULT_REPLY_EXPIRES = timedelta(days=1)

#: Format string for ``repr(app)``.
APP_REPR = """
<{name}({s.id}): {s.url} {s.state} actors({actors}) topics({topics})>
""".strip()

logger = get_logger(__name__)


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
            [self.app.topics],
            [self.app._fetcher],
        ))

    def _components_server(self) -> Iterable[ServiceT]:
        # Add all asyncio.Tasks, like timers, etc.
        for task in self.app._tasks:
            self.add_future(task())

        # Add the main Monitor sensor.
        self.app.monitor.beacon.reattach(self.beacon)
        self.app.monitor.loop = self.loop
        self.app.sensors.add(self.app.monitor)

        # Then return the list of "subservices",
        # those that'll be started when the app starts,
        # stopped when the app stops,
        # etc...
        return cast(Iterable[ServiceT], chain(
            # Sensors must always be started first, and stopped last.
            self.app.sensors,
            # Checkpoint Manager
            [self.app.checkpoints],                   # app.CheckpointManager
            # Producer must be stoppped after consumer.
            [self.app.producer],                      # app.Producer
            # Consumer must be stopped after Topic Manager
            [self.app.consumer],                      # app.Consumer
            # ReplyConsumer
            [self.app._reply_consumer],
            # Actors
            self.app.actors.values(),
            # TopicManager
            [self.app.topics],                        # app.TopicManager
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
        get = self.app._message_buffer.get
        while not self.should_stop:
            fut: FutureMessage = await get()
            pending = fut.message
            pending.channel.publish_message(fut)

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
        loop (asyncio.AbstractEventLoop):
            Provide specific asyncio event loop instance.
    """
    logger = logger

    client_only = False

    #: Default producer instance.
    _producer: Optional[ProducerT] = None

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
            checkpoint_path: Union[Path, str] = CHECKPOINT_PATH,
            key_serializer: CodecArg = 'json',
            value_serializer: CodecArg = 'json',
            num_standby_replicas: int = 0,
            replication_factor: int = 1,
            default_partitions: int = 8,
            reply_to: str = None,
            create_reply_topic: bool = False,
            reply_expires: Seconds = DEFAULT_REPLY_EXPIRES,
            Stream: SymbolArg[Type[StreamT]] = DEFAULT_STREAM_CLS,
            Table: SymbolArg[Type[TableT]] = DEFAULT_TABLE_CLS,
            TableManager: SymbolArg[Type[TableManagerT]] = DEFAULT_TABLE_MAN,
            CheckpointManager: SymbolArg[Type[CheckpointManagerT]] = _DCPM,
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
        self.checkpoint_path = Path(checkpoint_path)
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.num_standby_replicas = num_standby_replicas
        self.replication_factor = replication_factor
        self.default_partitions = default_partitions
        self.reply_to = reply_to or REPLY_TOPIC_PREFIX + str(uuid4())
        self.create_reply_topic = create_reply_topic
        self.reply_expires = want_seconds(
            reply_expires or DEFAULT_REPLY_EXPIRES)
        self.avro_registry_url = avro_registry_url
        self.Stream = symbol_by_name(Stream)
        self.TableType = symbol_by_name(Table)
        self.SetType = symbol_by_name(Set)
        self.TableManager = symbol_by_name(TableManager)
        self.CheckpointManager = symbol_by_name(CheckpointManager)
        self.Serializers = symbol_by_name(Serializers)
        self.serializers = self.Serializers(
            key_serializer=self.key_serializer,
            value_serializer=self.value_serializer,
        )
        self.advertised_url = ''
        self.assignor = PartitionAssignor(self,
                                          replicas=self.replication_factor)
        self.router = Router(self)
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
              key_serializer: CodecArg = None,
              value_serializer: CodecArg = None,
              partitions: int = None,
              retention: Seconds = None,
              compacting: bool = None,
              deleting: bool = None,
              replicas: int = None,
              acks: bool = True,
              config: Mapping[str, Any] = None) -> TopicT:
        return Topic(
            self,
            topics=topics,
            pattern=pattern,
            key_type=key_type,
            value_type=value_type,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            partitions=partitions,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
            replicas=replicas,
            acks=acks,
            config=config,
        )

    def channel(self, *,
                key_type: ModelArg = None,
                value_type: ModelArg = None,
                loop: asyncio.AbstractEventLoop = None) -> ChannelT:
        return Channel(
            self,
            key_type=key_type,
            value_type=value_type,
            loop=loop,
        )

    def actor(self,
              channel: Union[str, ChannelT] = None,
              *,
              name: str = None,
              concurrency: int = 1,
              sink: Iterable[SinkT] = None) -> Callable[[ActorFun], ActorT]:
        def _inner(fun: ActorFun) -> ActorT:
            actor = Actor(
                fun,
                name=name,
                app=self,
                channel=channel,
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
        """Create new stream from channel.

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
            app=self,
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
        table = self.tables.add(self.TableType(
            self,
            name=name,
            default=default,
            beacon=self.beacon,
            partitions=partitions,
            **kwargs))
        return table.using_window(window) if window else table

    def Set(self, name: str,
            *,
            window: WindowT = None,
            partitions: int = None,
            **kwargs: Any) -> SetT:
        return self.tables.add(self.SetType(
            self,
            name=name,
            beacon=self.beacon,
            partitions=partitions,
            window=window,
            **kwargs))

    async def start_client(self) -> None:
        self.client_only = True
        await self._service.maybe_start()

    async def maybe_start_client(self) -> None:
        if not self._service.started:
            await self.start_client()

    async def _maybe_attach(
            self,
            channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None,
            force: bool = False) -> Awaitable[RecordMetadata]:
        if not force:
            event = current_event()
            if event is not None:
                return cast(Event, event)._attach(
                    channel, key, value,
                    partition=partition,
                    key_serializer=key_serializer,
                    value_serializer=value_serializer,
                    callback=callback,
                )
        return await self.send(
            channel, key, value,
            partition=partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

    async def send(
            self,
            channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        """Send event to stream.

        Arguments:
            channel (Union[ChannelT, str]): Channel to send event to.
            key (K): Message key.
            value (V): Message value.
            partition (int): Specific partition to send to.
                If not set the partition will be chosen by the partitioner.
            key_serializer (CodecArg): Serializer to use
                only when key is not a model.
            value_serializer (CodecArg): Serializer to use
                only when value is not a model.
        """
        if isinstance(channel, str):
            channel = self.topic(channel)
        return await channel.send(
            key, value, partition,
            key_serializer, value_serializer, callback,
        )

    def send_soon(
            self,
            channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        """Send event to stream soon.

        This is for use by non-async functions.
        """
        chan = self.topic(channel) if isinstance(channel, str) else channel
        fut = chan.as_future_message(
            key, value, partition, key_serializer, value_serializer, callback)
        self._message_buffer.put(fut)
        return fut

    def _send_attached(
            self,
            message: Message,
            channel: Union[str, ChannelT],
            key: K,
            value: V,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        buf = self._pending_on_commit[message.tp]
        chan = self.topic(channel) if isinstance(channel, str) else channel
        fut = chan.as_future_message(
            key, value, partition,
            key_serializer, value_serializer, callback)
        heappush(buf, (message.offset, Unordered(fut)))
        return fut

    async def _commit_attached(self, tp: TopicPartition, offset: int) -> None:
        # publish pending messages attached to this TP+offset
        attached = list(self._get_attached(tp, offset))
        if attached:
            await asyncio.wait(
                [await fut.message.channel.publish_message(fut, wait=False)
                 for fut in attached],
                return_when=asyncio.ALL_COMPLETED,
                loop=self.loop,
            )

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

    @stampede
    async def maybe_start_producer(self) -> ProducerT:
        producer = self.producer
        # producer may also have been started by app.start()
        await producer.maybe_start()
        return producer

    async def commit(self, topics: TPorTopicSet) -> bool:
        return await self.topics.commit(topics)

    def _new_producer(self, beacon: NodeT = None) -> ProducerT:
        return self.transport.create_producer(
            beacon=beacon or self.beacon,
        )

    def _new_consumer(self) -> ConsumerT:
        return self.transport.create_consumer(
            callback=self.topics.on_message,
            on_partitions_revoked=self.on_partitions_revoked,
            on_partitions_assigned=self.on_partitions_assigned,
            beacon=self.beacon,
        )

    async def on_partitions_assigned(
            self, assigned: Iterable[TopicPartition]) -> None:
        await self.topics.on_partitions_assigned(assigned)
        await self.tables.on_partitions_assigned(assigned)
        self.flow_control.resume()

    async def on_partitions_revoked(
            self, revoked: Iterable[TopicPartition]) -> None:
        self.log.dev('ON PARTITIONS REVOKED')
        await self.topics.on_partitions_revoked(revoked)
        assignment = self.consumer.assignment()
        if assignment:
            self.flow_control.suspend()
            await self.consumer.pause_partitions(assignment)
            await self.consumer.wait_empty()
        else:
            self.log.dev('ON P. REVOKED NOT COMMITTING: ASSIGNMENT EMPTY')

    def _create_transport(self) -> TransportT:
        return transport.by_url(self.url)(self.url, self, loop=self.loop)

    def FlowControlQueue(
            self,
            maxsize: int = None,
            *,
            clear_on_resume: bool = False,
            loop: asyncio.AbstractEventLoop = None) -> asyncio.Queue:
        return FlowControlQueue(
            maxsize=maxsize,
            flow_control=self.flow_control,
            clear_on_resume=clear_on_resume,
            loop=loop or self.loop,
        )

    def __repr__(self) -> str:
        return APP_REPR.format(
            name=type(self).__name__,
            s=self,
            actors=self.actors,
            topics=len(self.topics),
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
    def checkpoints(self) -> CheckpointManagerT:
        return self.CheckpointManager(
            self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def topics(self) -> TopicManagerT:
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
        return self.FlowControlQueue(
            # it's important that we don't clear the buffered messages
            # on_partitions_assigned
            maxsize=1000, loop=self.loop, clear_on_resume=False)

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

    @cached_property
    def flow_control(self) -> FlowControlEvent:
        return FlowControlEvent(loop=self.loop)


_flake8_RecordMetadata_is_used: RecordMetadata  # XXX flake8 bug
