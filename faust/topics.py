"""Topic - Named channel using Kafka."""
import asyncio
import re
import typing
from functools import partial
from typing import (
    Any,
    Awaitable,
    Callable,
    Mapping,
    Optional,
    Pattern,
    Sequence,
    Set,
    Union,
    cast,
)
from mode import Seconds, get_logger
from mode.utils.futures import stampede
from mode.utils.queues import ThrowableQueue

from .channels import Channel
from .exceptions import KeyDecodeError, ValueDecodeError
from .events import Event
from .streams import current_event
from .types import (
    AppT,
    CodecArg,
    EventT,
    FutureMessage,
    K,
    Message,
    MessageSentCallback,
    ModelArg,
    PendingMessage,
    RecordMetadata,
    TP,
    V,
)
from .types.topics import ChannelT, TopicT
from .types.transports import ProducerT

if typing.TYPE_CHECKING:  # pragma: no cover
    from .app import App
else:
    class App: ...  # noqa

__all__ = ['Topic']

logger = get_logger(__name__)


class Topic(Channel, TopicT):
    """Define new topic description.

    Arguments:
        app: App instance used to create this topic description.

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

    _partitions: Optional[int] = None
    _pattern: Optional[Pattern] = None

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
        self.topics = topics or []
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

    async def send(self,
                   *,
                   key: K = None,
                   value: V = None,
                   partition: int = None,
                   key_serializer: CodecArg = None,
                   value_serializer: CodecArg = None,
                   callback: MessageSentCallback = None,
                   force: bool = False) -> Awaitable[RecordMetadata]:
        """Send message to topic."""
        if self.app._attachments.enabled and not force:
            event = current_event()
            if event is not None:
                return cast(Event, event)._attach(
                    self,
                    key,
                    value,
                    partition=partition,
                    key_serializer=key_serializer,
                    value_serializer=value_serializer,
                    callback=callback,
                )
        return await self._send_now(
            key,
            value,
            partition=partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

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
    def partitions(self) -> Optional[int]:
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
                              wait: bool = False) -> Awaitable[RecordMetadata]:
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
        pass

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
