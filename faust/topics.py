import asyncio
import re
import typing
from collections import defaultdict
from functools import total_ordering
from typing import (
    Any, AsyncIterator, Awaitable, Callable, Iterable, Iterator, Mapping,
    MutableMapping, Optional, Pattern, Set, Sequence, Type, Union, cast,
)
from .exceptions import KeyDecodeError, ValueDecodeError
from .types import (
    AppT, CodecArg, Message, ModelT, TopicPartition, K, V,
)
from .types.streams import StreamT, StreamCoroutine
from .types.topics import EventT, SourceT, TopicT, TopicManagerT
from .types.transports import ConsumerCallback, TPorTopicSet
from .utils.logging import get_logger
from .utils.services import Service
from .utils.times import Seconds

if typing.TYPE_CHECKING:
    from .app import App
else:
    class App: ...  # noqa

__all__ = [
    'Topic',
    'TopicT',
    'TopicSource',
    'SourceT',
    'TopicManager',
    'TopicManagerT',
]

logger = get_logger(__name__)

SENTINEL = object()


class Event(EventT):

    def __init__(self, app: AppT, key: K, value: V, message: Message) -> None:
        self.app: AppT = app
        self.key: K = key
        self.value: V = value
        self.message: Message = message

    async def send(self, topic: Union[str, TopicT],
                   *,
                   key: Any = SENTINEL) -> None:
        """Serialize and send object to topic."""
        if key is SENTINEL:
            key = self.key
        await self.app.send(topic, key, self.value)

    async def forward(self, topic: Union[str, TopicT],
                      *,
                      key: Any = SENTINEL) -> None:
        """Forward original message (will not be reserialized)."""
        if key is SENTINEL:
            key = self.key
        await self.app.send(topic, key, self.message.value)

    def attach(self, topic: Union[str, TopicT], key: K, value: V,
               *,
               partition: int = None,
               key_serializer: CodecArg = None,
               value_serializer: CodecArg = None) -> None:
        self.app.send_attached(
            self.message, topic, key, value,
            partition=partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )

    def ack(self) -> None:
        message = self.message
        # decrement the reference count
        message.decref()
        # if no more references, ack message
        if not message.refcount:
            self.app.sources.ack_message(message)

    async def __aenter__(self) -> EventT:
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        self.ack()

    def __enter__(self) -> EventT:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.ack()


@total_ordering
class Topic(TopicT):
    """Define new topic description.

    Arguments:
        app (AppT): App instance this topic is bound to.

    Keyword Arguments:
        topics: Sequence[str]: List of topic names.
        partitions: int: Number of partitions for these topics.
            On declaration, topics are created using this.
            Note: kafka cluster configuration is used if message produced
            when topic not declared.
        retention
        pattern (Union[str, Pattern]): Regular expression to match.
            You cannot specify both topics and a pattern.
        key_type (Type[ModelT]): Model used for keys in this topic.
        value_type (Type[ModelT]): Model used for values in this topic.

    Raises:
        TypeError: if both `topics` and `pattern` is provided.

    Returns:
        faust.types.Topic: a named tuple.

    """

    _declared = False

    def __init__(self, app: AppT,
                 *,
                 topics: Sequence[str] = None,
                 pattern: Union[str, Pattern] = None,
                 key_type: Type[ModelT] = None,
                 value_type: Type[ModelT] = None,
                 partitions: int = None,
                 retention: Seconds = None,
                 compacting: bool = None,
                 deleting: bool = None,
                 config: Mapping[str, Any] = None) -> None:
        if pattern and topics:
            raise TypeError('Cannot specify both topics and pattern.')
        if isinstance(pattern, str):
            pattern = re.compile(pattern)
        self.topics = topics
        self.partitions = partitions or app.default_partitions
        self.replicas = app.replication_factor
        self.retention = retention
        self.compacting = compacting
        self.deleting = deleting
        self.config = config or {}
        self.app = app
        self.pattern = pattern
        self.key_type = key_type
        self.value_type = value_type

    def stream(self, coroutine: StreamCoroutine = None,
               **kwargs: Any) -> StreamT:
        return self.app.stream(self, coroutine, **kwargs)

    async def send(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            *,
            wait: bool = True) -> Awaitable:
        return await self.app.send(
            self, key, value, partition,
            key_serializer, value_serializer,
            wait=wait,
        )

    def send_soon(self, key: K, value: V,
                  partition: int = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None) -> None:
        return self.app.send_soon(
            self, key, value, partition,
            key_serializer, value_serializer)

    def derive(self,
               *,
               topics: Sequence[str] = None,
               key_type: Type[ModelT] = None,
               value_type: Type[ModelT] = None,
               partitions: int = None,
               retention: Seconds = None,
               compacting: bool = None,
               deleting: bool = None,
               config: Mapping[str, Any] = None,
               prefix: str = '',
               suffix: str = '') -> TopicT:
        if self.pattern:
            raise ValueError('Cannot add suffix to Topic with pattern')
        if topics is None:
            topics = self.topics
        return type(self)(
            self.app,
            topics=[f'{prefix}{topic}{suffix}' for topic in topics],
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
        for topic in self.topics:
            await cast(App, self.app).producer.create_topic(
                topic=topic,
                partitions=self.partitions,
                replication=self.replicas,
                config=self.config,
            )

    def __aiter__(self) -> AsyncIterator:
        source = TopicSource(self)
        self.app.sources.add(source)
        return source

    async def __anext__(self) -> Any:
        # XXX Mypy seems to think AsyncIterable sould have __anext__
        raise NotImplementedError('here because of a mypy bug')

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self}>'

    def __str__(self) -> str:
        return str(self.pattern) if self.pattern else ','.join(self.topics)

    def __lt__(self, other: Any) -> bool:
        # LT is here for heapq.heappush in app.send_attached
        if isinstance(other, TopicT):
            if self.pattern:
                return self.pattern.pattern < other.pattern.pattern
            return tuple(self.topics) < tuple(other.topics)
        return False


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
        return f'<{type(self).__name__}: {self.topic!r}'


class TopicManager(TopicManagerT, Service):
    """Manages the sources that subscribe to topics.

    - Consumes messages from topic using a single consumer.
    - Forwards messages to all sources subscribing to a topic.
    """

    #: Fast index to see if source is registered.
    _sources: Set[SourceT]

    #: Map str topic to set of streams that should get a copy
    #: of each message sent to that topic.
    _topicmap: MutableMapping[str, Set[SourceT]]

    _pending_tasks: asyncio.Queue

    #: Whenever a change is made, i.e. a source is added/removed, we notify
    #: the background task responsible for resubscribing.
    _subscription_changed: Optional[asyncio.Event]

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app
        self._sources = set()
        self._topicmap = defaultdict(set)
        self._pending_tasks = asyncio.Queue(loop=self.loop)

        self._subscription_changed = None
        # we compile the closure used for receive messages
        # (this just optimizes symbol lookups, localizing variables etc).
        self.on_message = self._compile_message_handler()

    def ack_message(self, message: Message) -> None:
        if not message.acked:
            return self.ack_offset(message.tp, message.offset)
        message.acked = True

    def ack_offset(self, tp: TopicPartition, offset: int) -> None:
        return self.app.consumer.ack(tp, offset)

    async def commit(self, topics: TPorTopicSet) -> bool:
        return await self.app.consumer.commit(topics)

    def _compile_message_handler(self) -> ConsumerCallback:
        wait = asyncio.wait
        return_when = asyncio.ALL_COMPLETED
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
            await wait(
                [add_pending_task(source.deliver(message))
                 for source in sources],
                loop=loop,
                return_when=return_when,
            )
        return on_message

    @Service.task
    async def _subscriber(self) -> None:
        # the first time we start, we will wait two seconds
        # to give actors a chance to start up and register their
        # streams.  This way we won't have N subscription requests at the
        # start.
        await self.sleep(2.0)

        # then we compile the subscription topic pattern,
        self._compile_pattern()

        # tell the consumer to subscribe to our pattern
        await self.app.consumer.subscribe(self._pattern)

        # Now we wait for changes
        ev = self._subscription_changed = asyncio.Event(loop=self.loop)
        while not self.should_stop:
            await ev.wait()
            self._compile_pattern()
            await self.app.consumer.subscribe(self._pattern)
            ev.clear()

    @Service.task
    async def _gatherer(self) -> None:
        waiting = set()
        wait = asyncio.wait
        return_when = asyncio.FIRST_COMPLETED
        while not self.should_stop:
            waiting.add(await self._pending_tasks.get())
            finished, unfinished = await wait(waiting, return_when=return_when)
            waiting = unfinished

    def _compile_pattern(self) -> None:
        self._topicmap.clear()
        for source in self._sources:
            for topic in source.topic.topics:
                self._topicmap[topic].add(source)
        self._pattern = '^' + '$|^'.join(self._topicmap) + '$'

    def on_partitions_assigned(
            self, assigned: Iterable[TopicPartition]) -> None:
        ...

    def on_partitions_revoked(
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
            if self._subscription_changed is not None:
                self._subscription_changed.set()

    def discard(self, source: SourceT) -> None:
        self._sources.discard(source)
        self.beacon.discard(source)
        if self._subscription_changed is not None:
            self._subscription_changed.set()

    @property
    def label(self) -> str:
        return f'{type(self).__name__}({len(self._sources)})'
