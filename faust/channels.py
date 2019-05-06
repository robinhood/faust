"""Channel.

A channel is used to send values to streams.

The stream will iterate over incoming events in the channel.

"""
import asyncio

from typing import (
    Any,
    Awaitable,
    Callable,
    Mapping,
    MutableSet,
    Optional,
    Set,
    cast,
)
from weakref import WeakSet

from mode import Seconds, get_logger, want_seconds
from mode.utils.futures import maybe_async, stampede
from mode.utils.queues import ThrowableQueue

from .events import Event
from .types import (
    AppT,
    ChannelT,
    CodecArg,
    EventT,
    FutureMessage,
    K,
    Message,
    MessageSentCallback,
    ModelArg,
    PendingMessage,
    RecordMetadata,
    StreamT,
    TP,
    V,
)
from .types.core import HeadersArg, OpenHeadersArg, prepare_headers
from .types.tuples import _PendingMessage_to_Message

__all__ = ['Channel']

logger = get_logger(__name__)


class Channel(ChannelT):
    """Create new channel.

    Arguments:
        app: The app that created this channel (``app.channel()``)

        key_type:  The Model used for keys in this channel.
        value_type: The Model used for values in this channel.
        maxsize: The maximum number of messages this channel can hold.
                 If exceeded any new ``put`` call will block until a message
                 is removed from the channel.
        is_iterator: When streams iterate over a channel they will call
            ``stream.clone(is_iterator=True)`` so this attribute
            denotes that this channel instance is currently being iterated
            over.
        active_partition: Set of active topic partitions this
           channel instance is assigned to.
        loop: The :mod:`asyncio` event loop to use.
    """

    app: AppT
    key_type: Optional[ModelArg]
    value_type: Optional[ModelArg]
    is_iterator: bool

    _queue: Optional[ThrowableQueue]
    _root: Optional['Channel']
    _subscribers: MutableSet['Channel']

    def __init__(self,
                 app: AppT,
                 *,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 is_iterator: bool = False,
                 queue: ThrowableQueue = None,
                 maxsize: int = None,
                 root: ChannelT = None,
                 active_partitions: Set[TP] = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.app = app
        self.loop = loop
        self.key_type = key_type
        self.value_type = value_type
        self.is_iterator = is_iterator
        self._queue = queue
        self.maxsize = maxsize
        self.deliver = self._compile_deliver()  # type: ignore
        self._root = cast(Channel, root)
        self.active_partitions = active_partitions
        self._subscribers = WeakSet()

    @property
    def queue(self) -> ThrowableQueue:
        """Return the underlying queue/buffer backing this channel."""
        if self._queue is None:
            # this should only be set after clone = channel.__aiter__()
            # which means the loop is not accessed by merely defining
            # a channel at module scope.
            maxsize = self.maxsize
            if maxsize is None:
                maxsize = self.app.conf.stream_buffer_maxsize
            self._queue = self.app.FlowControlQueue(
                maxsize=maxsize,
                loop=self.loop,
                clear_on_resume=True,
            )
        return self._queue

    def clone(self, *, is_iterator: bool = None, **kwargs: Any) -> ChannelT:
        """Create clone of this channel.

        Arguments:
            is_iterator: Set to True if this is now a channel
                that is being iterated over.

        Keyword Arguments:
            **kwargs: Any keyword arguments passed will override any
                of the arguments supported by
                :class:`Channel.__init__ <Channel>`.
        """
        is_it = is_iterator if is_iterator is not None else self.is_iterator
        subchannel: ChannelT = self._clone(is_iterator=is_it, **kwargs)
        if is_it:
            (self._root or self)._subscribers.add(cast(Channel, subchannel))
        # make sure queue is created at this point
        # ^ it's a cached_property
        subchannel.queue
        return subchannel

    def clone_using_queue(self, queue: asyncio.Queue) -> ChannelT:
        """Create clone of this channel using specific queue instance."""
        return self.clone(queue=queue, is_iterator=True)

    def _clone(self, **kwargs: Any) -> ChannelT:
        return type(self)(**{**self._clone_args(), **kwargs})

    def _clone_args(self) -> Mapping:
        # How to create a copy of this channel.
        return {
            'app': self.app,
            'loop': self.loop,
            'key_type': self.key_type,
            'value_type': self.value_type,
            'maxsize': self.maxsize,
            'root': self._root if self._root is not None else self,
            'queue': None,
            'active_partitions': self.active_partitions,
        }

    def stream(self, **kwargs: Any) -> StreamT:
        """Create stream reading from this channel."""
        return self.app.stream(self, **kwargs)

    def get_topic_name(self) -> str:
        """Get the topic name, or raise if this is not a named channel."""
        raise NotImplementedError('Channels are unnamed topics')

    async def send(self,
                   *,
                   key: K = None,
                   value: V = None,
                   partition: int = None,
                   timestamp: float = None,
                   headers: HeadersArg = None,
                   key_serializer: CodecArg = None,
                   value_serializer: CodecArg = None,
                   callback: MessageSentCallback = None,
                   force: bool = False) -> Awaitable[RecordMetadata]:
        """Send message to channel."""
        return await self._send_now(
            key,
            value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

    def send_soon(self,
                  *,
                  key: K = None,
                  value: V = None,
                  partition: int = None,
                  timestamp: float = None,
                  headers: HeadersArg = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None,
                  callback: MessageSentCallback = None,
                  force: bool = False) -> FutureMessage:
        """Produce message by adding to buffer.

        This method is only supported by :class:`~faust.Topic`.

        Raises:
            NotImplementedError: always for in-memory channel.
        """
        raise NotImplementedError()

    def as_future_message(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            timestamp: float = None,
            headers: HeadersArg = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> FutureMessage:
        """Create promise that message will be transmitted."""
        return FutureMessage(
            PendingMessage(
                self,
                self.prepare_key(key, key_serializer),
                self.prepare_value(value, value_serializer),
                key_serializer=key_serializer,
                value_serializer=value_serializer,
                partition=partition,
                timestamp=timestamp,
                headers=self.prepare_headers(headers),
                callback=callback,
                # Python 3.6.0: NamedTuple doesn't support optional fields
                # [ask]
                topic=None,
                offset=None,
            ),
        )

    def prepare_headers(
            self, headers: Optional[HeadersArg]) -> OpenHeadersArg:
        """Prepare ``headers`` passed before publishing."""
        if headers is not None:
            return prepare_headers(headers)
        return {}

    async def _send_now(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            timestamp: float = None,
            headers: HeadersArg = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        return await self.publish_message(
            self.as_future_message(
                key, value, partition, timestamp, headers,
                key_serializer, value_serializer, callback))

    async def publish_message(self, fut: FutureMessage,
                              wait: bool = True) -> Awaitable[RecordMetadata]:
        """Publish message to channel.

        This is the interface used by ``topic.send()``, etc.
        to actually publish the message on the channel
        after being buffered up or similar.

        It takes a :class:`~faust.types.FutureMessage` object,
        which contains all the information required to send
        the message, and acts as a promise that is resolved
        once the message has been fully transmitted.
        """
        event = self._create_event(
            fut.message.key, fut.message.value, fut.message.headers,
            message=_PendingMessage_to_Message(fut.message))
        await self.put(event)
        topic, partition = tp = TP(
            fut.message.topic or '<anon>',
            fut.message.partition or -1)
        return await self._finalize_message(
            fut, RecordMetadata(
                topic=topic,
                partition=partition,
                topic_partition=tp,
                offset=-1,
                timestamp=fut.message.timestamp,
                timestamp_type=1,
            ),
        )

    async def _finalize_message(self, fut: FutureMessage,
                                result: RecordMetadata) -> FutureMessage:
        fut.set_result(result)
        if fut.message.callback:
            await maybe_async(fut.message.callback(fut))
        return fut

    @stampede
    async def maybe_declare(self) -> None:
        """Declare/create this channel, but only if it doesn't exist."""
        ...

    async def declare(self) -> None:
        """Declare/create this channel.

        This is used to create this channel on a server,
        if that is required to operate it.
        """
        ...

    def prepare_key(self, key: K, key_serializer: CodecArg) -> Any:
        """Prepare key before it is sent to this channel.

        :class:`~faust.Topic` uses this to implement serialization of keys
        sent to the channel.
        """
        return key

    def prepare_value(self, value: V, value_serializer: CodecArg) -> Any:
        """Prepare value before it is sent to this channel.

        :class:`~faust.Topic` uses this to implement serialization of values
        sent to the channel.
        """
        return value

    async def decode(self, message: Message, *,
                     propagate: bool = False) -> EventT:
        """Decode :class:`~faust.types.Message` into :class:`~faust.Event`."""
        return self._create_event(
            message.key, message.value, message.headers, message=message)

    async def deliver(self, message: Message) -> None:  # pragma: no cover
        """Deliver message to queue from consumer.

        This is called by the consumer to deliver the message
        to the channel.
        """
        ...  # closure compiled at __init__

    def _compile_deliver(self) -> Callable[[Message], Awaitable[None]]:
        put = None

        async def deliver(message: Message) -> None:
            nonlocal put
            if put is None:
                # NOTE circumvents self.put, using queue directly
                put = self.queue.put
            event = await self.decode(message)
            await put(event)

        return deliver

    def _create_event(self,
                      key: K,
                      value: V,
                      headers: Optional[HeadersArg],
                      message: Message) -> EventT:
        return Event(self.app, key, value, headers, message)

    async def put(self, value: Any) -> None:
        """Put event onto this channel."""
        root = self._root if self._root is not None else self
        for subscriber in root._subscribers:
            await subscriber.queue.put(value)

    async def get(self, *, timeout: Seconds = None) -> Any:
        """Get the next :class:`~faust.Event` received on this channel."""
        timeout_: float = want_seconds(timeout)
        if timeout_:
            return await asyncio.wait_for(self.queue.get(), timeout=timeout_)
        return await self.queue.get()

    def empty(self) -> bool:
        """Return :const:`True` if the queue is empty."""
        return self.queue.empty()

    async def on_key_decode_error(self, exc: Exception,
                                  message: Message) -> None:
        """Unable to decode the key of an item in the queue.

        See Also:
            :meth:`on_decode_error`
        """
        await self.on_decode_error(exc, message)
        await self.throw(exc)

    async def on_value_decode_error(self, exc: Exception,
                                    message: Message) -> None:
        """Unable to decode the value of an item in the queue.

        See Also:
            :meth:`on_decode_error`
        """
        await self.on_decode_error(exc, message)
        await self.throw(exc)

    async def on_decode_error(self, exc: Exception, message: Message) -> None:
        """Signal that there was an error reading an event in the queue.

        When a message in the channel needs deserialization
        to be reconstructed back to its original form, we will sometimes
        see decoding/deserialization errors being raised, from missing
        fields or malformed payloads, and so on.

        We will log the exception, but you can also override this
        to perform additional actions.

        Admonition: Kafka
            In the event a deserialization error occurs, we
            HAVE to commit the offset of the source message to continue
            processing the stream.

            For this reason it is important that you keep a close eye on
            error logs. For easy of use, we suggest using log aggregation
            software, such as Sentry, to surface these errors to your
            operations team.
        """
        ...

    def on_stop_iteration(self) -> None:
        """Signal that iteration over this channel was stopped.

        Tip:
            Remember to call ``super`` when overriding this method.
        """
        ...

    def derive(self, **kwargs: Any) -> ChannelT:
        """Derive new channel from this channel, using new configuration.

        See :class:`faust.Topic.derive`.

        For local channels this will simply return the same channel.
        """
        return self

    def __aiter__(self) -> ChannelT:
        return self if self.is_iterator else self.clone(is_iterator=True)

    async def __anext__(self) -> EventT:
        if not self.is_iterator:
            raise RuntimeError('Need to call channel.__aiter__()')
        return await self.queue.get()

    async def throw(self, exc: BaseException) -> None:
        """Throw exception to be received by channel subscribers.

        Tip:
            When you find yourself having to call this from
            a regular, non-``async def`` function, you can use :meth:`_throw`
            instead.
        """
        self.queue._throw(exc)

    def _throw(self, exc: BaseException) -> None:
        """Non-async version of :meth:`throw`."""
        self.queue._throw(exc)

    def __repr__(self) -> str:
        s = f'<{self.label}@{self._object_id_as_hex()}'
        if self.active_partitions is not None:
            # if we are restricted to a specific set of topic partitions,
            # then include that in repr(channel).
            if self.active_partitions:
                active = '{' + ', '.join(sorted(
                    f'{tp.topic}:{tp.partition}'
                    for tp in self.active_partitions)) + '}'
            else:
                # a defined, but empty .active_partitions signifies
                # that we are still waiting for an assignment
                # from the Consumer.
                active = '{<pending for assignment>}'
            s += f' active={active}'
        s += '>'
        return s

    def _object_id_as_hex(self) -> str:
        # hexadecimal version of id(self)
        return f'{id(self):#x}'

    def __str__(self) -> str:
        # subclasses should override this
        return '<ANON>'

    @property
    def subscriber_count(self) -> int:
        """Return number of active subscribers to local channel."""
        return len(self._subscribers)

    @property
    def label(self) -> str:
        """Short textual description of channel."""
        sym = '(*)' if self.is_iterator else ''
        return f'{sym}{type(self).__name__}: {self}'
