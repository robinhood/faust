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
    Tuple,
    TypeVar,
    cast,
    no_type_check,
)
from weakref import WeakSet

from mode import Seconds, get_logger, want_seconds
from mode.utils.futures import maybe_async, stampede
from mode.utils.queues import ThrowableQueue

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
    SchemaT,
    StreamT,
    TP,
    V,
)
from .types.core import HeadersArg, OpenHeadersArg, prepare_headers
from .types.tuples import _PendingMessage_to_Message

__all__ = ['Channel']

logger = get_logger(__name__)

T = TypeVar('T')
T_contra = TypeVar('T_contra', contravariant=True)


class Channel(ChannelT[T]):
    """Create new channel.

    Arguments:
        app: The app that created this channel (``app.channel()``)

        schema: Schema used for serialization/deserialization
        key_type:  The Model used for keys in this channel.
           (overrides schema if one is defined)
        value_type: The Model used for values in this channel.
           (overrides schema if one is defined)
        maxsize: The maximum number of messages this channel can hold.
                 If exceeded any new ``put`` call will block until a message
                 is removed from the channel.
        is_iterator: When streams iterate over a channel they will call
            ``stream.clone(is_iterator=True)`` so this attribute
            denotes that this channel instance is currently being iterated
            over.
        active_partitions: Set of active topic partitions this
           channel instance is assigned to.
        loop: The :mod:`asyncio` event loop to use.
    """

    app: AppT
    schema: SchemaT
    key_type: Optional[ModelArg]
    value_type: Optional[ModelArg]
    is_iterator: bool

    _queue: Optional[ThrowableQueue]
    _root: Optional['Channel']
    _subscribers: MutableSet['Channel']

    def __init__(self,
                 app: AppT,
                 *,
                 schema: SchemaT = None,
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
        self.is_iterator = is_iterator
        self._queue = queue
        self.maxsize = maxsize
        self.deliver = self._compile_deliver()  # type: ignore
        self._root = cast(Channel, root)
        self.active_partitions = active_partitions
        self._subscribers = WeakSet()

        if schema is None:
            self.schema = self._get_default_schema(key_type, value_type)
        else:
            self.schema = schema
            self.schema.update(key_type=key_type, value_type=value_type)
        self.key_type = self.schema.key_type
        self.value_type = self.schema.value_type

    def _get_default_schema(self,
                            key_type: ModelArg = None,
                            value_type: ModelArg = None) -> SchemaT:
        return self.app.conf.Schema(
            key_type=key_type,
            value_type=value_type,
        )

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

    def clone(self, *, is_iterator: bool = None, **kwargs: Any) -> ChannelT[T]:
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

    def clone_using_queue(self, queue: asyncio.Queue) -> ChannelT[T]:
        """Create clone of this channel using specific queue instance."""
        return self.clone(queue=queue, is_iterator=True)

    def _clone(self, **kwargs: Any) -> ChannelT[T]:
        return type(self)(**{**self._clone_args(), **kwargs})

    def _clone_args(self) -> Mapping:
        # How to create a copy of this channel.
        return {
            'app': self.app,
            'loop': self.loop,
            'schema': self.schema,
            'key_type': self.key_type,
            'value_type': self.value_type,
            'maxsize': self.maxsize,
            'root': self._root if self._root is not None else self,
            'queue': None,
            'active_partitions': self.active_partitions,
        }

    def stream(self, **kwargs: Any) -> StreamT[T]:
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
                   schema: SchemaT = None,
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
            schema=schema,
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
                  schema: SchemaT = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None,
                  callback: MessageSentCallback = None,
                  force: bool = False,
                  eager_partitioning: bool = False) -> FutureMessage:
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
            schema: SchemaT = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None,
            eager_partitioning: bool = False) -> FutureMessage:
        """Create promise that message will be transmitted."""
        open_headers = self.prepare_headers(headers)
        final_key, open_headers = self.prepare_key(
            key, key_serializer, schema, open_headers)
        final_value, open_headers = self.prepare_value(
            value, value_serializer, schema, open_headers)
        if partition is None and eager_partitioning:
            # Note: raises NotImplementedError if used on unnamed channel.
            partition = self.app.producer.key_partition(
                self.get_topic_name(), final_key).partition
        return FutureMessage(
            PendingMessage(
                self,
                final_key,
                final_value,
                key_serializer=key_serializer,
                value_serializer=value_serializer,
                partition=partition,
                timestamp=timestamp,
                headers=open_headers,
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
            schema: SchemaT = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        return await self.publish_message(
            self.as_future_message(
                key, value, partition, timestamp, headers,
                schema, key_serializer, value_serializer, callback))

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
        event = self._future_message_to_event(fut)
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

    def _future_message_to_event(self, fut: FutureMessage) -> EventT:
        return self._create_event(
            fut.message.key, fut.message.value, fut.message.headers,
            message=_PendingMessage_to_Message(fut.message))

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

    def prepare_key(
            self,
            key: K,
            key_serializer: CodecArg,
            schema: SchemaT = None,
            headers: OpenHeadersArg = None) -> Tuple[Any, OpenHeadersArg]:
        """Prepare key before it is sent to this channel.

        :class:`~faust.Topic` uses this to implement serialization of keys
        sent to the channel.
        """
        return key, headers

    def prepare_value(
            self,
            value: V,
            value_serializer: CodecArg,
            schema: SchemaT = None,
            headers: OpenHeadersArg = None) -> Tuple[Any, OpenHeadersArg]:
        """Prepare value before it is sent to this channel.

        :class:`~faust.Topic` uses this to implement serialization of values
        sent to the channel.
        """
        return value, headers

    async def decode(self, message: Message, *,
                     propagate: bool = False) -> EventT[T]:
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
                      message: Message) -> EventT[T]:
        return self.app.create_event(key, value, headers, message)

    async def put(self, value: EventT[T_contra]) -> None:
        """Put event onto this channel."""
        root = self._root if self._root is not None else self
        for subscriber in root._subscribers:
            await subscriber.queue.put(value)

    async def get(self, *, timeout: Seconds = None) -> EventT[T]:
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

    def derive(self, **kwargs: Any) -> ChannelT[T]:
        """Derive new channel from this channel, using new configuration.

        See :class:`faust.Topic.derive`.

        For local channels this will simply return the same channel.
        """
        return self

    def __aiter__(self) -> ChannelT[T]:
        return self if self.is_iterator else self.clone(is_iterator=True)

    async def __anext__(self) -> EventT[T]:
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


class SerializedChannel(Channel[T]):

    def __init__(self,
                 app: AppT,
                 *,
                 schema: SchemaT = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = None,
                 allow_empty: bool = None,
                 **kwargs: Any) -> None:
        self.app = app  # need to set early
        if schema is not None:
            self._contribute_to_schema(
                schema,
                key_type=key_type,
                value_type=value_type,
                key_serializer=key_serializer,
                value_serializer=value_serializer,
                allow_empty=allow_empty,
            )
        else:
            schema = self._get_default_schema(
                key_type, value_type,
                key_serializer, value_serializer,
                allow_empty)

        super().__init__(
            app,
            schema=schema,
            key_type=key_type,
            value_type=value_type,
            **kwargs,
        )
        self.key_serializer = self.schema.key_serializer
        self.value_serializer = self.schema.value_serializer
        self.allow_empty = self.schema.allow_empty

    def _contribute_to_schema(self, schema: SchemaT, *,
                              key_type: ModelArg = None,
                              value_type: ModelArg = None,
                              key_serializer: CodecArg = None,
                              value_serializer: CodecArg = None,
                              allow_empty: bool = None) -> None:
        # Update schema and take compat attributes
        # from passed schema.
        schema.update(
            key_type=key_type,
            value_type=value_type,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            allow_empty=allow_empty,
        )

    def _get_default_schema(self,
                            key_type: ModelArg = None,
                            value_type: ModelArg = None,
                            key_serializer: CodecArg = None,
                            value_serializer: CodecArg = None,
                            allow_empty: bool = None) -> SchemaT:
        return self.app.conf.Schema(
            key_type=key_type,
            value_type=value_type,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            allow_empty=allow_empty,
        )

    @no_type_check  # incompatible with base class, but OK
    def _clone_args(self) -> Mapping:
        return {
            **super()._clone_args(),
            **{
                'key_serializer': self.key_serializer,
                'value_serializer': self.value_serializer,
            },
        }

    def prepare_key(
            self,
            key: K,
            key_serializer: CodecArg,
            schema: SchemaT = None,
            headers: OpenHeadersArg = None) -> Tuple[Any, OpenHeadersArg]:
        """Serialize key to format suitable for transport."""
        if key is not None:
            schema = schema or self.schema
            assert schema is not None
            return schema.dumps_key(self.app, key,
                                    serializer=key_serializer,
                                    headers=headers)
        return None, headers

    def prepare_value(
            self,
            value: V,
            value_serializer: CodecArg,
            schema: SchemaT = None,
            headers: OpenHeadersArg = None) -> Tuple[Any, OpenHeadersArg]:
        """Serialize value to format suitable for transport."""
        schema = schema or self.schema
        assert schema is not None
        return schema.dumps_value(self.app, value,
                                  serializer=value_serializer,
                                  headers=headers)
