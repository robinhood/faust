import asyncio
import re
from collections import defaultdict
from functools import total_ordering
from typing import (
    Any, AsyncIterator, Awaitable, MutableMapping, Pattern,
    Set, Sequence, Type, Union,
)
from .types import (
    AppT, CodecArg, Message, TopicPartition, K, V,
)
from .types.streams import StreamT, StreamCoroutine
from .types.topics import EventT, TopicT, TopicConsumerT, TopicManagerT
from .types.transports import ConsumerCallback, ConsumerT, TPorTopicSet
from .utils.logging import get_logger
from .utils.services import Service

__all__ = [
    'Topic',
    'TopicT',
    'TopicConsumer',
    'TopicConsumerT',
    'TopicManager',
    'TopicManagerT',
]

logger = get_logger(__name__)

SENTINEL = object()


class Event(EventT):

    app: AppT
    key: K
    value: V
    message: Message

    async def send(self, topic: Union[str, TopicT],
                   *,
                   key: Any = SENTINEL) -> None:
        """Serialize and send object to topic."""
        if key is SENTINEL:
            key = self.key
        await self.app.send(topic, key, self)

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
        pattern (Union[str, Pattern]): Regular expression to match.
            You cannot specify both topics and a pattern.
        key_type (Type): Model used for keys in this topic.
        value_type (Type): Model used for values in this topic.

    Raises:
        TypeError: if both `topics` and `pattern` is provided.

    Returns:
        faust.types.Topic: a named tuple.

    """

    def __init__(self, app: AppT,
                 *,
                 topics: Sequence[str] = None,
                 pattern: Union[str, Pattern] = None,
                 key_type: Type = None,
                 value_type: Type = None) -> None:
        if pattern and topics:
            raise TypeError('Cannot specify both topics and pattern.')
        if isinstance(pattern, str):
            pattern = re.compile(pattern)
        self.topics = topics
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
               key_type: Type = None,
               value_type: Type = None,
               prefix: str = '',
               suffix: str = '',
               format: str = '{prefix}{topic}{suffix}') -> TopicT:
        if self.pattern:
            raise ValueError('Cannot add suffix to Topic with pattern')
        if topics is None:
            topics = self.topics
        return type(self)(
            self.app,
            topics=[
                format.format(prefix=prefix, topic=topic, suffix=suffix)
                for topic in topics
            ],
            pattern=self.pattern,
            key_type=self.key_type if key_type is None else key_type,
            value_type=self.value_type if value_type is None else value_type,
        )

    def __aiter__(self) -> AsyncIterator:
        return TopicConsumer(self)

    async def __anext__(self) -> Any:
        # XXX Mypy seems to think AsyncIterable sould have __anext__
        raise NotImplementedError('here because of a mypy bug')

    def __repr__(self) -> str:
        return '<{name}: {self}>'.format(
            name=type(self).__name__,
            self=self)

    def __str__(self) -> str:
        return str(self.pattern) if self.pattern else ','.join(self.topics)

    def __lt__(self, other: Any) -> bool:
        # LT is here for heapq.heappush in app.send_attached
        if isinstance(other, TopicT):
            a = self.pattern if self.pattern else self.topics
            b = other.pattern if other.pattern else other.topics
            return a < b
        return False


class TopicConsumer(TopicConsumerT, Service):
    queue: asyncio.Queue

    def __init__(self, topic: TopicT, **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.topic = topic
        self.app = self.topic.app
        self.queue = asyncio.Queue(loop=self.loop)

    async def deliver(self, message: Message) -> None:
        app = self.app
        topic = self.topic
        # deserialize key+value and convert to Event
        loads_key = self.app.serializers.loads_key
        loads_value = self.app.serializers.loads_value
        k = v = None
        try:
            k = await loads_key(topic.key_type, message.key)
        except Exception as exc:
            await self.on_key_decode_error(exc, message)
        else:
            try:
                v = await loads_value(topic.value_type, k, message)
            except Exception as exc:
                await self.on_value_decode_error(exc, message)
            await self.put(Event(app, k, v, message))

    async def put(self, event: EventT) -> None:
        await self.queue.put(event)

    async def on_key_decode_error(
            self, exc: Exception, message: Message) -> None:
        logger.error('Cannot decode key: %r: %r', message.key, exc)

    async def on_value_decode_error(
            self, exc: Exception, message: Message) -> None:
        logger.error('Cannot decode value for key=%r (%r): %r',
                     message.key, message.value, exc)

    async def get(self) -> EventT:
        return await self.queue.get()

    def __aiter__(self) -> AsyncIterator:
        self.app.add_source(self)
        return self

    async def __anext__(self) -> EventT:
        return await self.queue.get()

    def _repr_info(self) -> str:
        return repr(self.topic)


class TopicManager(TopicManagerT, Service):
    """Manages the sources that subscribe to topics.

    - Consumes messages from topic using a single consumer.
    - Forwards messages to all sources subscribing to a topic.
    """

    #: Fast index to see if source is registered.
    _sources: Set[TopicConsumerT]

    #: Map str topic to set of streams that should get a copy
    #: of each message sent to that topic.
    _topicmap: MutableMapping[str, Set[TopicConsumerT]]

    _pending_tasks: asyncio.Queue

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        self.consumer = None
        self._sources = set()
        self._topicmap = defaultdict(set)
        self._pending_tasks = asyncio.Queue(loop=self.loop)
        super().__init__(**kwargs)

    def ack_message(self, message: Message) -> None:
        if not message.acked:
            return self.ack_offset(
                TopicPartition(message.topic, message.partition),
                message.offset,
            )
        message.acked = True

    def ack_offset(self, tp: TopicPartition, offset: int) -> None:
        return self.consumer.ack(tp, offset)

    async def commit(self, topics: TPorTopicSet) -> bool:
        return await self.consumer.commit(topics)

    def add_source(self, source: TopicConsumerT) -> None:
        if source not in self._sources:
            self._sources.add(source)
            self.beacon.add(source)  # connect to beacon

    async def update(self) -> None:
        self._compile_pattern()
        await self.consumer.subscribe(self._pattern)

    def _compile_message_handler(self) -> ConsumerCallback:
        gather = asyncio.gather
        list_ = list
        # topic str -> list of TopicConsumer
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
            # for TopicConsumer.__anext__ to pick up.
            await gather(*[
                add_pending_task(source.deliver(message))
                for source in sources
            ], loop=self.loop)
        return on_message

    async def on_start(self) -> None:
        self.add_future(self._delayed_start())
        self.add_future(self._gatherer())

    async def _delayed_start(self) -> None:
        # wait for tasks to start streams
        await asyncio.sleep(2.0, loop=self.loop)

        # then register topics etc.
        self._compile_pattern()
        self._on_message = self._compile_message_handler()
        self.consumer = self._create_consumer()
        await self.consumer.subscribe(self._pattern)
        await self.consumer.start()

    async def _gatherer(self) -> None:
        waiting = set()
        wait = asyncio.wait
        return_when = asyncio.FIRST_COMPLETED
        while not self.should_stop:
            waiting.add(await self._pending_tasks.get())
            finished, unfinished = await wait(waiting, return_when=return_when)
            waiting = unfinished

    async def on_stop(self) -> None:
        if self.consumer:
            await self.consumer.stop()

    def _create_consumer(self) -> ConsumerT:
        return self.app.transport.create_consumer(
            callback=self._on_message,
            on_partitions_revoked=self._on_partitions_revoked,
            on_partitions_assigned=self._on_partitions_assigned,
            beacon=self.beacon,
        )

    def _compile_pattern(self) -> None:
        self._topicmap.clear()
        for source in self._sources:
            for topic in source.topic.topics:
                self._topicmap[topic].add(source)
        self._pattern = '|'.join(self._topicmap)

    def _on_partitions_assigned(self,
                                assigned: Sequence[TopicPartition]) -> None:
        ...

    def _on_partitions_revoked(self,
                               revoked: Sequence[TopicPartition]) -> None:
        ...

    def __len__(self) -> int:
        return len(self._sources)

    @property
    def label(self) -> str:
        return '{}({})'.format(
            type(self).__name__, len(self._sources))
