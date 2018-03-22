"""Topic - Named channel using Kafka."""
import asyncio
import re
import typing
from collections import defaultdict
from functools import partial
from time import monotonic
from typing import (
    Any,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSet,
    Optional,
    Pattern,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)
from mode import Seconds, Service, get_logger
from mode.utils.futures import ThrowableQueue, notify, stampede

from .channels import Channel
from .exceptions import KeyDecodeError, ValueDecodeError
from .types import (
    AppT,
    CodecArg,
    ConsumerT,
    EventT,
    FutureMessage,
    K,
    Message,
    ModelArg,
    PendingMessage,
    RecordMetadata,
    TP,
    V,
)
from .types.topics import ChannelT, ConductorT, TopicT
from .types.transports import ConsumerCallback, ProducerT, TPorTopicSet
from .types.tuples import tp_set_to_map

if typing.TYPE_CHECKING:
    from .app import App
else:
    class App: ...  # noqa

__all__ = ['Topic', 'TopicConductor']

logger = get_logger(__name__)


class Topic(Channel, TopicT):
    """Define new topic description.

    Arguments:
        app: App instance this topic is bound to.

        topics: List of topic names.
        partitions: Number of partitions for these topics.
                    On declaration, topics are created using this.
                    Note: If a message is produced before the topic is
                    declared, and ``autoCreateTopics`` is enabled on
                    the Kafka Server, the number of partitions used
                    will be specified by the server configuration.
        retention: Number of seconds (as float/timedelta) to keep messages
                   in the topic before they can be expired by the server.
        pattern: Regular expression evaluated to decide what topics to
                 subscribe to. You cannot specify both topics and a pattern.
        key_type: How to deserialize keys for messages in this topic.
                  Can be a :class:`faust.Model` type, :class:`str`,
                  :class:`bytes`, or :const:`None` for "autodetect"
        value_type: How to deserialize values for messages in this topic.
                  Can be a :class:`faust.Model` type, :class:`str`,
                  :class:`bytes`, or :const:`None` for "autodetect"
        active_partitions: Set of :class:`faust.types.tuples.TP` that this
                  topic should be restricted to.

    Raises:
        TypeError: if both `topics` and `pattern` is provided.
    """

    _partitions: int = None
    _pattern: Pattern = None

    def __init__(self,
                 app: AppT,
                 *,
                 topics: Sequence[str] = None,
                 pattern: Union[str, Pattern] = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 is_iterator: bool = False,
                 partitions: int = None,
                 retention: Seconds = None,
                 compacting: bool = None,
                 deleting: bool = None,
                 replicas: int = None,
                 acks: bool = True,
                 internal: bool = False,
                 config: Mapping[str, Any] = None,
                 queue: ThrowableQueue = None,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = None,
                 maxsize: int = None,
                 root: ChannelT = None,
                 active_partitions: Set[TP] = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.topics = topics
        super().__init__(
            app,
            key_type=key_type,
            value_type=value_type,
            loop=loop,
            is_iterator=is_iterator,
            queue=queue,
            maxsize=maxsize,
        )
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.pattern = cast(Pattern, pattern)
        self.partitions = partitions
        self.retention = retention
        self.compacting = compacting
        self.deleting = deleting
        self.replicas = replicas
        self.acks = acks
        self.internal = internal
        self.active_partitions = active_partitions
        self.config = config or {}

    async def decode(self, message: Message, *,
                     propagate: bool = False) -> EventT:
        # first call to decode compiles and caches it.
        decode = self.decode = self._compile_decode()  # type: ignore
        return await decode(message)

    async def put(self, event: EventT) -> None:
        if not self.is_iterator:
            raise RuntimeError(
                f'Cannot put on Topic channel before aiter({self})')
        await self.queue.put(event)

    def _compile_decode(self) -> Callable[[Message], Awaitable[EventT]]:
        app = self.app
        key_type = self.key_type
        value_type = self.value_type
        loads_key = app.serializers.loads_key
        loads_value = app.serializers.loads_value
        create_event = self._create_event
        key_serializer = self.key_serializer
        value_serializer = self.value_serializer

        async def decode(message: Message, *, propagate: bool = False) -> Any:
            try:
                k = loads_key(key_type, message.key, serializer=key_serializer)
            except KeyDecodeError as exc:
                if propagate:
                    raise
                await self.on_key_decode_error(exc, message)
            else:
                try:
                    v = loads_value(
                        value_type, message.value, serializer=value_serializer)
                except ValueDecodeError as exc:
                    if propagate:
                        raise
                    await self.on_value_decode_error(exc, message)
                else:
                    return create_event(k, v, message)

        return decode

    def _clone_args(self) -> Mapping:
        return {
            **super()._clone_args(),
            **{
                'topics': self.topics,
                'pattern': self.pattern,
                'partitions': self.partitions,
                'retention': self.retention,
                'compacting': self.compacting,
                'deleting': self.deleting,
                'replicas': self.replicas,
                'internal': self.internal,
                'key_serializer': self.key_serializer,
                'value_serializer': self.value_serializer,
                'acks': self.acks,
                'config': self.config,
                'active_partitions': self.active_partitions}}

    @property
    def pattern(self) -> Optional[Pattern]:
        return self._pattern

    @pattern.setter
    def pattern(self, pattern: Union[str, Pattern]) -> None:
        if pattern and self.topics:
            raise TypeError('Cannot specify both topics and pattern')
        self._pattern = re.compile(pattern) if pattern else None

    @property
    def partitions(self) -> int:
        return self._partitions

    @partitions.setter
    def partitions(self, partitions: int) -> None:
        if partitions == 0:
            raise ValueError('Topic cannot have zero partitions')
        self._partitions = partitions

    def derive(self, **kwargs: Any) -> ChannelT:
        """Create new :class:`Topic` derived from this topic.

        Configuration will be copied from this topic, but any parameter
        overriden as a keyword argument.

        See Also:
            :meth:`derive_topic`: for a list of supported keyword arguments.
        """
        return self.derive_topic(**kwargs)

    def derive_topic(self,
                     *,
                     topics: Sequence[str] = None,
                     key_type: ModelArg = None,
                     value_type: ModelArg = None,
                     key_serializer: CodecArg = None,
                     value_serializer: CodecArg = None,
                     partitions: int = None,
                     retention: Seconds = None,
                     compacting: bool = None,
                     deleting: bool = None,
                     internal: bool = None,
                     config: Mapping[str, Any] = None,
                     prefix: str = '',
                     suffix: str = '',
                     **kwargs: Any) -> TopicT:
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
            key_serializer=(
                self.key_serializer
                if key_serializer is None else key_serializer),
            value_serializer=(
                self.value_serializer
                if value_serializer is None else value_serializer),
            partitions=self.partitions if partitions is None else partitions,
            retention=self.retention if retention is None else retention,
            compacting=self.compacting if compacting is None else compacting,
            deleting=self.deleting if deleting is None else deleting,
            config=self.config if config is None else config,
            internal=self.internal if internal is None else internal,
        )

    def get_topic_name(self) -> str:
        if self.pattern:
            raise TypeError(
                'Topic with pattern subscription cannot be identified')
        if self.topics:
            if len(self.topics) > 1:
                raise ValueError(
                    'Topic with multiple topic names cannot be identified')
            return self.topics[0]
        raise TypeError('Topic has no subscriptions (no pattern, no topics)')

    async def _get_producer(self) -> ProducerT:
        return await self.app.maybe_start_producer()

    async def publish_message(self, fut: FutureMessage,
                              wait: bool = True) -> Awaitable[RecordMetadata]:
        app = self.app
        message: PendingMessage = fut.message
        if isinstance(message.channel, str):
            topic = message.channel
        elif isinstance(message.channel, TopicT):
            topic = cast(TopicT, message.channel).get_topic_name()
        else:
            topic = self.get_topic_name()
        key: bytes = cast(bytes, message.key)
        value: bytes = cast(bytes, message.value)
        logger.debug('send: topic=%r key=%r value=%r', topic, key, value)
        assert topic is not None
        producer = await self._get_producer()
        state = app.sensors.on_send_initiated(
            producer,
            topic,
            keysize=len(key) if key else 0,
            valsize=len(value) if value else 0)
        if wait:
            ret: RecordMetadata = await producer.send_and_wait(
                topic, key, value, partition=message.partition)
            app.sensors.on_send_completed(producer, state)
            return await self._finalize_message(fut, ret)
        else:
            fut2 = await producer.send(
                topic, key, value, partition=message.partition)
            cast(asyncio.Future, fut2).add_done_callback(
                cast(Callable, partial(self._on_published, message=fut)))
            return fut2

    def _on_published(self, fut: asyncio.Future,
                      message: FutureMessage) -> None:
        res: RecordMetadata = fut.result()
        message.set_result(res)
        if message.message.callback:
            message.message.callback(message)

    def prepare_key(self, key: K, key_serializer: CodecArg) -> Any:
        if key is not None:
            return self.app.serializers.dumps_key(
                self.key_type,
                key,
                serializer=key_serializer or self.key_serializer)
        return None

    def prepare_value(self, value: V, value_serializer: CodecArg) -> Any:
        return self.app.serializers.dumps_value(
            self.value_type,
            value,
            serializer=value_serializer or self.value_serializer)

    def on_stop_iteration(self) -> None:
        self.app.topics.discard(self)

    @stampede
    async def maybe_declare(self) -> None:
        await self.declare()

    async def declare(self) -> None:
        producer = await self._get_producer()
        for topic in self.topics:
            partitions = self.partitions
            if partitions is None:
                partitions = self.app.conf.topic_partitions
            replicas = self.replicas
            if replicas is None:
                replicas = self.app.conf.topic_replication_factor
            await producer.create_topic(
                topic=topic,
                partitions=partitions,
                replication=replicas,
                config=self.config,
                compacting=self.compacting,
                deleting=self.deleting,
                retention=self.retention,
            )

    def __aiter__(self) -> ChannelT:
        if self.is_iterator:
            return self
        else:
            channel = self.clone(is_iterator=True)
            self.app.topics.add(channel)
            return channel

    def __str__(self) -> str:
        return str(self.pattern) if self.pattern else ','.join(self.topics)


class TopicConductor(ConductorT, Service):
    """Manages the channels that subscribe to topics.

    - Consumes messages from topic using a single consumer.
    - Forwards messages to all channels subscribing to a topic.
    """

    logger = logger

    #: Fast index to see if Topic is registered.
    _topics: MutableSet[TopicT]

    #: Map str topic to set of channels that should get a copy
    #: of each message sent to that topic.
    _topicmap: MutableMapping[str, MutableSet[TopicT]]

    #: Map of TP to set of channels that should get a copy
    #: of each message sent to that TP.
    #: Anything in this set, overrides _topicmap
    _tp_direct: MutableMapping[TP, MutableSet[TopicT]]

    #: Whenever a change is made, i.e. a Topic is added/removed, we notify
    #: the background task responsible for resubscribing.
    _subscription_changed: Optional[asyncio.Event]

    _subscription_done: Optional[asyncio.Future]

    _acking_topics: Set[str]

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app
        self._topics = set()
        self._topicmap = defaultdict(set)
        self._tp_direct = defaultdict(set)
        self._acking_topics = set()
        self._subscription_changed = None
        self._subscription_done = None
        # we compile the closure used for receive messages
        # (this just optimizes symbol lookups, localizing variables etc).
        self.on_message: Callable[[Message], Awaitable[None]]
        self.on_message = self._compile_message_handler()

    async def commit(self, topics: TPorTopicSet) -> bool:
        return await self.app.consumer.commit(topics)

    def _compile_message_handler(self) -> ConsumerCallback:
        # This method localizes variables and attribute access
        # for better performance.  This is part of the inner loop
        # of a Faust worker, so tiny improvements here has big impact.

        # topic str -> list of TopicT
        get_channels_for_topic = self._topicmap.__getitem__
        consumer: ConsumerT = None
        on_message_in = self.app.sensors.on_message_in
        on_topic_buffer_full = self.app.sensors.on_topic_buffer_full
        unacked: Set[Message] = None
        add_unacked: Callable[[Message], None] = None
        acquire_flow_control: Callable = self.app.flow_control.acquire
        len_: Callable[[Any], int] = len
        acking_topics: Set[str] = self._acking_topics

        async def on_message(message: Message) -> None:
            nonlocal consumer, unacked, add_unacked
            if consumer is None:
                consumer = self.app.consumer
                unacked = consumer._unacked_messages
                add_unacked = unacked.add
            # when a message is received we find all channels
            # that subscribe to this message
            tp = message.tp
            topic = message.topic
            try:
                channels = cast(Set[Topic], self._tp_direct[tp])
            except KeyError:
                channels = cast(Set[Topic], get_channels_for_topic(topic))
            channels_n = len_(channels)
            await acquire_flow_control()
            if channels_n:
                # we increment the reference count for this message in bulk
                # immediately, so that nothing will get a chance to decref to
                # zero before we've had the chance to pass it to all channels
                message.incref(channels_n)
                if topic in acking_topics:
                    # This inlines Consumer.track_message(message)
                    add_unacked(message)
                    on_message_in(message.tp, message.offset, message)
                    # XXX ugh this should be in the consumer somehow
                    if consumer._last_batch is None:
                        # set last_batch received timestamp if not already set.
                        # the commit livelock monitor uses this to check
                        # how long between receiving a message to we commit it
                        # (we reset _last_batch to None in .commit())
                        consumer._last_batch = monotonic()

                event: EventT = None
                event_keyid: Tuple[K, V] = None

                # forward message to all channels subscribing to this topic

                # keep track of the number of channels we delivered to,
                # so that if a DecodeError is raised we can propagate
                # that errors to the remaining channels.
                delivered: Set[Topic] = set()
                full: List[Tuple[EventT, Topic]] = []
                try:
                    for chan in channels:
                        keyid = chan.key_type, chan.value_type
                        if event is None:
                            # first channel deserializes the payload:
                            event = await chan.decode(message, propagate=True)
                            event_keyid = keyid

                            queue = chan.queue
                            if queue.full():
                                full.append((event, chan))
                                continue
                            queue.put_nowait(event)
                        else:
                            # subsequent channels may have a different
                            # key/value type pair, meaning they all can
                            # deserialize the message in different ways

                            dest_event: EventT
                            if keyid == event_keyid:
                                # Reuse the event if it uses the same keypair:
                                dest_event = event
                            else:
                                dest_event = await chan.decode(
                                    message, propagate=True)
                            queue = chan.queue
                            if queue.full():
                                full.append((dest_event, chan))
                                continue
                            queue.put_nowait(dest_event)
                        delivered.add(chan)
                    if full:
                        for _, dest_chan in full:
                            on_topic_buffer_full(dest_chan)
                        await asyncio.wait(
                            [dest_chan.put(dest_event)
                             for dest_event, dest_chan in full],
                            return_when=asyncio.ALL_COMPLETED,
                        )
                except KeyDecodeError as exc:
                    remaining = channels - delivered
                    message.ack(self.app.consumer, n=len(remaining))
                    for channel in remaining:
                        await channel.on_key_decode_error(exc, message)
                        delivered.add(channel)
                except ValueDecodeError as exc:
                    remaining = channels - delivered
                    message.ack(self.app.consumer, n=len(remaining))
                    for channel in remaining:
                        await channel.on_value_decode_error(exc, message)
                        delivered.add(channel)

        return on_message

    def acks_enabled_for(self, topic: str) -> bool:
        return topic in self._acking_topics

    @Service.task
    async def _subscriber(self) -> None:
        # the first time we start, we will wait two seconds
        # to give agents a chance to start up and register their
        # streams.  This way we won't have N subscription requests at the
        # start.
        await self.sleep(2.0)

        # tell the consumer to subscribe to the topics.
        await self.app.consumer.subscribe(await self._update_topicmap())
        notify(self._subscription_done)

        # Now we wait for changes
        ev = self._subscription_changed = asyncio.Event(loop=self.loop)
        while not self.should_stop:
            await ev.wait()
            await self.app.consumer.subscribe(await self._update_topicmap())
            ev.clear()
            notify(self._subscription_done)

    async def wait_for_subscriptions(self) -> None:
        if self._subscription_done is not None:
            await self._subscription_done

    async def _update_topicmap(self) -> Iterable[str]:
        self._topicmap.clear()
        for channel in self._topics:
            if channel.internal:
                await channel.maybe_declare()
            for topic in channel.topics:
                if channel.acks:
                    self._acking_topics.add(topic)
                self._topicmap[topic].add(channel)
        return self._topicmap

    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        self._tp_direct.clear()
        assignmap = tp_set_to_map(assigned)
        for topic in self._topics:
            if topic.active_partitions is not None:
                if topic.active_partitions:
                    assert topic.active_partitions.issubset(assigned)
                    for tp in topic.active_partitions:
                        self._tp_direct[tp].add(topic)
            else:
                for subtopic in topic.topics:
                    for tp in assignmap[subtopic]:
                        self._tp_direct[tp].add(topic)

    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        self._tp_direct.clear()

    def clear(self) -> None:
        self._topics.clear()
        self._topicmap.clear()
        self._acking_topics.clear()

    def __contains__(self, value: Any) -> bool:
        return value in self._topics

    def __iter__(self) -> Iterator[TopicT]:
        return iter(self._topics)

    def __len__(self) -> int:
        return len(self._topics)

    def __hash__(self) -> int:
        return object.__hash__(self)

    def add(self, topic: Any) -> None:
        if topic not in self._topics:
            self._topics.add(topic)
            self._flag_changes()

    def discard(self, topic: Any) -> None:
        self._topics.discard(topic)
        self.beacon.discard(topic)
        self._flag_changes()

    def _flag_changes(self) -> None:
        if self._subscription_changed is not None:
            self._subscription_changed.set()
        if self._subscription_done is None:
            self._subscription_done = asyncio.Future(loop=self.loop)

    @property
    def label(self) -> str:
        return f'{type(self).__name__}({len(self._topics)})'

    @property
    def shortlabel(self) -> str:
        return type(self).__name__
