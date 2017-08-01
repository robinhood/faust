import asyncio
import re
import typing
from collections import defaultdict
from types import TracebackType
from typing import (
    Any, AsyncIterator, Awaitable, Callable, Iterable, Iterator, Mapping,
    MutableMapping, Optional, Pattern, Sequence, Set, Type, Union, cast,
)
from .exceptions import KeyDecodeError, ValueDecodeError
from .types import (
    AppT, CodecArg, K, Message, MessageSentCallback,
    ModelArg, RecordMetadata, TopicPartition, V,
)
from .types.streams import StreamCoroutine, StreamT
from .types.topics import EventT, SourceT, TopicManagerT, TopicT
from .types.transports import ConsumerCallback, TPorTopicSet
from .utils.futures import notify
from .utils.logging import get_logger
from .utils.services import Service
from .utils.times import Seconds

if typing.TYPE_CHECKING:
    from .app import App
else:
    class App: ...  # noqa

__all__ = [
    'Topic',
    'TopicSource',
    'TopicManager',
]

logger = get_logger(__name__)

USE_EXISTING_KEY = object()


class Event(EventT):
    """An event in a stream.

    You can retrieve the current event in a stream to:

        - Get the app associated with the value.
        - Attach messages to be published when the offset of the
          value's message source is committed.
        - Get access to the serialized key+value.
        - Get access to message properties like, what topic+partition
          the value was received on, or its offset.

    A stream can iterate over any value, but when the value is received
    as a message in a topic, the current value in a stream will be associated
    with an :class:`Event`.

    The TopicManager passes messages in a topic to a stream as events::

        topic_source: TopicSource

        event = Event(app, deserialized_key, deserialized_value, message)
        await topic_source.deliver(event)

    The steam iterates over the TopicSource and receives the event,
    but iterating over the stream yields raw values:

        async for value in stream:   # <- this gets event.value, not event
            ...

    You can get the event for the current value in a stream by accessing
    ``stream.current_event``::

        async for value in stream:
            event = stream.current_event

    Note that if you want access to both key and value, you should use
    ``stream.items()`` instead::

        async for key, value in stream.items():
            ...
    """

    def __init__(self, app: AppT, key: K, value: V, message: Message) -> None:
        self.app: AppT = app
        self.key: K = key
        self.value: V = value
        self.message: Message = message

    async def send(self, topic: Union[str, TopicT],
                   *,
                   key: Any = USE_EXISTING_KEY) -> RecordMetadata:
        """Serialize and send object to topic."""
        if key is USE_EXISTING_KEY:
            key = self.key
        return await self.app.send(topic, key, self.value)

    async def forward(self, topic: Union[str, TopicT],
                      *,
                      key: Any = USE_EXISTING_KEY) -> None:
        """Forward original message (will not be reserialized)."""
        if key is USE_EXISTING_KEY:
            key = self.message.key
        await self.app.send(topic, key=key, value=self.message.value)

    def attach(self, topic: Union[str, TopicT], key: K, value: V,
               *,
               partition: int = None,
               key_serializer: CodecArg = None,
               value_serializer: CodecArg = None,
               callback: MessageSentCallback = None) -> None:
        self.app.send_attached(
            self.message, topic, key, value,
            partition=partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

    async def ack(self) -> None:
        message = self.message
        # decrement the reference count
        message.decref()
        # if no more references, ack message
        if not message.refcount:
            await self.app.consumer.ack(message)

    def __repr__(self) -> str:
        return f'{type(self).__name__}: k={self.key!r} v={self.value!r}'

    async def __aenter__(self) -> EventT:
        return self

    async def __aexit__(self,
                        exc_type: Type[Exception],
                        exc_val: Exception,
                        exc_tb: TracebackType) -> None:
        await self.ack()


class Topic(TopicT):
    """Define new topic description.

    Arguments:
        app (AppT): App instance this topic is bound to.

    Keyword Arguments:
        topics: List of topic names.
        partitions: Number of partitions for these topics.
            On declaration, topics are created using this.
            Note: kafka cluster configuration is used if message produced
            when topic not declared.
        retention: Number of seconds (float/timedelta) to keep messages
            in the topic before they expire.
        pattern: Regular expression to match.
            You cannot specify both topics and a pattern.
        key_type: Model used for keys in this topic.
        value_type: Model used for values in this topic.

    Raises:
        TypeError: if both `topics` and `pattern` is provided.
    """

    _declared = False
    _partitions: int = None
    _replicas: int = None
    _pattern: Pattern = None

    def __init__(self, app: AppT,
                 *,
                 topics: Sequence[str] = None,
                 pattern: Union[str, Pattern] = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 partitions: int = None,
                 retention: Seconds = None,
                 compacting: bool = None,
                 deleting: bool = None,
                 replicas: int = None,
                 acks: bool = True,
                 config: Mapping[str, Any] = None) -> None:
        self.app = app
        self.topics = topics
        self.pattern = cast(Pattern, pattern)  # XXX mypy does not read setter
        self.key_type = key_type
        self.value_type = value_type
        self.partitions = partitions
        self.retention = retention
        self.compacting = compacting
        self.deleting = deleting
        self.replicas = replicas
        self.config = config or {}

    @property
    def pattern(self) -> Optional[Pattern]:
        return self._pattern

    @pattern.setter
    def pattern(self, pattern: Union[str, Pattern]) -> None:
        if pattern and self.topics:
            raise TypeError('Cannot specify both topics and pattern')
        if isinstance(pattern, str):
            pattern = re.compile(pattern)
        self._pattern = pattern

    @property
    def partitions(self) -> int:
        return self._partitions

    @partitions.setter
    def partitions(self, partitions: int) -> None:
        if partitions is None:
            partitions = self.app.default_partitions
        if partitions == 0:
            raise ValueError('Topic cannot have 0 (zero partitions)')
        self._partitions = partitions

    @property
    def replicas(self) -> int:
        return self._replicas

    @replicas.setter
    def replicas(self, replicas: int) -> None:
        if replicas is None:
            replicas = self.app.replication_factor
        self._replicas = replicas

    def stream(self, coroutine: StreamCoroutine = None,
               **kwargs: Any) -> StreamT:
        """Create stream from topic."""
        return self.app.stream(self, coroutine, **kwargs)

    async def send(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None) -> RecordMetadata:
        """Send message to topic."""
        return await self.app.send(
            self, key, value, partition,
            key_serializer, value_serializer,
        )

    def send_soon(self, key: K, value: V,
                  partition: int = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None) -> None:
        """Send message to topic (asynchronous version).

        Notes:
            This can be used from non-async functions, but with the caveat
            that sending of the message will be scheduled in the event loop.
            This will buffer up the message, making backpressure a concern.
        """
        return self.app.send_soon(
            self, key, value, partition,
            key_serializer, value_serializer)

    def derive(self,
               *,
               topics: Sequence[str] = None,
               key_type: ModelArg = None,
               value_type: ModelArg = None,
               partitions: int = None,
               retention: Seconds = None,
               compacting: bool = None,
               deleting: bool = None,
               config: Mapping[str, Any] = None,
               prefix: str = '',
               suffix: str = '') -> TopicT:
        """Create new :class:`Topic` derived from this topic.

        Configuration will be copied from this topic, but any parameter
        overriden as a keyword argument.
        """
        topics = self.topics if topics is None else topics
        if suffix or prefix:
            if self.pattern:
                raise ValueError(
                    'Cannot add prefix/suffix to Topic with pattern')
                topics = [f'{prefix}{topic}{suffix}' for topic in topics]
        return type(self)(
            self.app,
            topics=topics,
            pattern=self.pattern,
            key_type=self.key_type if key_type is None else key_type,
            value_type=self.value_type if value_type is None else value_type,
            partitions=self.partitions if partitions is None else partitions,
            retention=self.retention if retention is None else retention,
            compacting=self.compacting if compacting is None else compacting,
            deleting=self.deleting if deleting is None else deleting,
            config=self.config if config is None else config,
        )

    async def maybe_declare(self) -> None:
        if not self._declared:
            self._declared = True
            await self.declare()

    async def declare(self) -> None:
        producer = await self.app.maybe_start_producer()
        for topic in self.topics:
            await producer.create_topic(
                topic=topic,
                partitions=self.partitions,
                replication=self.replicas,
                config=self.config,
            )

    def __aiter__(self) -> AsyncIterator:
        source = TopicSource(self)
        self.app.sources.add(source)
        return source

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self}>'

    def __str__(self) -> str:
        return str(self.pattern) if self.pattern else ','.join(self.topics)


class TopicSource(SourceT):
    queue: asyncio.Queue

    def __init__(self, topic: TopicT,
                 *,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.topic = topic
        self.app = self.topic.app
        self.loop = loop or self.app.loop
        self.queue = asyncio.Queue(loop=self.loop)
        self.deliver = self._compile_deliver()  # type: ignore

    async def deliver(self, message: Message) -> None:
        ...  # closure compiled at __init__

    def _compile_deliver(self) -> Callable[[Message], Awaitable]:
        app = self.app
        topic = self.topic
        key_type = topic.key_type
        value_type = topic.value_type
        loads_key = self.app.serializers.loads_key
        loads_value = self.app.serializers.loads_value
        put = self.queue.put  # NOTE circumvents self.put, using queue directly
        create_event = Event

        async def deliver(message: Message) -> None:
            try:
                k = await loads_key(key_type, message.key)
            except KeyDecodeError as exc:
                await self.on_key_decode_error(exc, message)
            else:
                try:
                    v = await loads_value(value_type, message.value)
                except ValueDecodeError as exc:
                    await self.on_value_decode_error(exc, message)
                else:
                    await put(create_event(app, k, v, message))
        return deliver

    async def put(self, value: Any) -> None:
        await self.queue.put(value)

    async def get(self) -> Any:
        return await self.queue.get()

    async def on_key_decode_error(
            self, exc: Exception, message: Message) -> None:
        logger.exception('Cannot decode key: %r: %r', message.key, exc)

    async def on_value_decode_error(
            self, exc: Exception, message: Message) -> None:
        logger.exception('Cannot decode value for key=%r (%r): %r',
                         message.key, message.value, exc)

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self) -> EventT:
        return await self.queue.get()

    def __repr__(self) -> str:
        return f'<{self.label}>'

    @property
    def label(self) -> str:
        return f'{type(self).__name__}: {self.topic!r}'


class TopicManager(TopicManagerT, Service):
    """Manages the sources that subscribe to topics.

    - Consumes messages from topic using a single consumer.
    - Forwards messages to all sources subscribing to a topic.
    """
    logger = logger

    #: Fast index to see if source is registered.
    _sources: Set[SourceT]

    #: Map str topic to set of streams that should get a copy
    #: of each message sent to that topic.
    _topicmap: MutableMapping[str, Set[SourceT]]

    _pending_tasks: asyncio.Queue

    #: Whenever a change is made, i.e. a source is added/removed, we notify
    #: the background task responsible for resubscribing.
    _subscription_changed: Optional[asyncio.Event]

    _subscription_done: Optional[asyncio.Future]

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app
        self._sources = set()
        self._topicmap = defaultdict(set)
        self._pending_tasks = asyncio.Queue(loop=self.loop)

        self._subscription_changed = None
        self._subscription_done = None
        # we compile the closure used for receive messages
        # (this just optimizes symbol lookups, localizing variables etc).
        self.on_message: Callable[[Message], Awaitable[None]]
        self.on_message = self._compile_message_handler()

    async def commit(self, topics: TPorTopicSet) -> bool:
        return await self.app.consumer.commit(topics)

    def _compile_message_handler(self) -> ConsumerCallback:
        wait = asyncio.wait
        all_completed = asyncio.ALL_COMPLETED
        loop = self.loop
        list_ = list
        # topic str -> list of TopicSource
        get_sources_for_topic = self._topicmap.__getitem__

        add_pending_task = self._pending_tasks.put

        async def on_message(message: Message) -> None:
            # when a message is received we find all sources
            # that subscribe to this message
            sources = list_(get_sources_for_topic(message.topic))

            # we increment the reference count for this message in bulk
            # immediately, so that nothing will get a chance to decref to
            # zero before we've had the chance to pass it to all sources
            message.incref_bulk(sources)

            # Then send it to each sources inbox,
            # for TopicSource.__anext__ to pick up.
            # NOTE: We do this in parallel, so the order of sources'
            #       does not matter.
            await wait(
                [add_pending_task(source.deliver(message))
                 for source in sources],
                loop=loop,
                return_when=all_completed,
            )
        return on_message

    @Service.task
    async def _subscriber(self) -> None:
        # the first time we start, we will wait two seconds
        # to give actors a chance to start up and register their
        # streams.  This way we won't have N subscription requests at the
        # start.
        await self.sleep(2.0)

        # tell the consumer to subscribe to the topics.
        await self.app.consumer.subscribe(self._update_topicmap())
        notify(self._subscription_done)

        # Now we wait for changes
        ev = self._subscription_changed = asyncio.Event(loop=self.loop)
        while not self.should_stop:
            await ev.wait()
            await self.app.consumer.subscribe(self._update_topicmap())
            ev.clear()
            notify(self._subscription_done)

    async def wait_for_subscriptions(self) -> None:
        if self._subscription_done is not None:
            await self._subscription_done

    @Service.task
    async def _gatherer(self) -> None:
        waiting = set()
        wait = asyncio.wait
        first_completed = asyncio.FIRST_COMPLETED
        while not self.should_stop:
            waiting.add(await self._pending_tasks.get())
            finished, unfinished = await wait(
                waiting, return_when=first_completed)
            waiting = unfinished

    def _update_topicmap(self) -> Iterable[str]:
        self._topicmap.clear()
        for source in self._sources:
            for topic in source.topic.topics:
                self._topicmap[topic].add(source)
        return self._topicmap

    async def on_partitions_assigned(
            self, assigned: Iterable[TopicPartition]) -> None:
        ...

    async def on_partitions_revoked(
            self, revoked: Iterable[TopicPartition]) -> None:
        ...

    def __contains__(self, value: Any) -> bool:
        return value in self._sources

    def __iter__(self) -> Iterator[SourceT]:
        return iter(self._sources)

    def __len__(self) -> int:
        return len(self._sources)

    def __hash__(self) -> int:
        return object.__hash__(self)

    def add(self, source: SourceT) -> None:
        if source not in self._sources:
            self._sources.add(source)
            self.beacon.add(source)  # connect to beacon
            self._flag_changes()

    def discard(self, source: SourceT) -> None:
        self._sources.discard(source)
        self.beacon.discard(source)
        self._flag_changes()

    def _flag_changes(self) -> None:
        if self._subscription_changed is not None:
            self._subscription_changed.set()
        if self._subscription_done is None:
            self._subscription_done = asyncio.Future(loop=self.loop)

    @property
    def label(self) -> str:
        return f'{type(self).__name__}({len(self._sources)})'

    @property
    def shortlabel(self) -> str:
        return type(self).__name__
