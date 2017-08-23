import asyncio
from types import TracebackType
from typing import Any, Awaitable, Callable, Mapping, Type, Union
from .exceptions import KeyDecodeError, ValueDecodeError
from .streams import current_event
from .types import (
    AppT, CodecArg, FutureMessage, K, Message,
    MessageSentCallback, ModelArg, PendingMessage, RecordMetadata, V,
)
from .types.channels import ChannelT, EventT
from .types.streams import StreamCoroutine, StreamT
from .utils.futures import maybe_async
from .utils.logging import get_logger

__all__ = ['Event', 'Channel']
logger = get_logger(__name__)

USE_EXISTING_KEY = object()


class Event(EventT):
    """An event in a stream.

    You can retrieve the current event in a stream to:

        - Get the app associated with the value.
        - Attach messages to be published when the offset of the
          value's source message is committed.
        - Get access to the serialized key+value.
        - Get access to message properties like, what topic+partition
          the value was received on, or its offset.

    A stream can iterate over any value, but when the value is received
    as a message in a topic, the current value in a stream will be associated
    with an :class:`Event`.

    Streams iterates over messages in a channel, and the TopicManager
    is the service that feeds the channel with messages.  The objects
    sent tot he channel are Event objects::

        channel: ChannelT

        event = Event(app, deserialized_key, deserialized_value, message)
        await channel.deliver(event)

    The stream iterates over the Channel and receives the event,
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

    async def send(self, topic: Union[str, ChannelT],
                   *,
                   key: Any = USE_EXISTING_KEY,
                   force: bool = False) -> FutureMessage:
        """Serialize and send object to topic."""
        return await self._send(topic, key, self.value, force=force)

    async def forward(self, topic: Union[str, ChannelT],
                      *,
                      key: Any = USE_EXISTING_KEY,
                      force: bool = False) -> FutureMessage:
        """Forward original message (will not be reserialized)."""
        return await self._send(
            topic, key=key, value=self.message.value, force=force)

    async def _send(self, topic: Union[str, ChannelT],
                    key: Any = USE_EXISTING_KEY,
                    value: Any = None,
                    force: bool = False) -> FutureMessage:
        if key is USE_EXISTING_KEY:
            key = self.message.key
        return await self.app.maybe_attach(topic, key, value, force=force)

    def attach(self,
               channel: Union[ChannelT, str],
               key: K = None,
               value: V = None,
               partition: int = None,
               key_serializer: CodecArg = None,
               value_serializer: CodecArg = None,
               callback: MessageSentCallback = None) -> FutureMessage:
        return self.app.send_attached(
            self.message, channel, key, value,
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


class Channel(ChannelT):
    app: AppT
    key_type: ModelArg
    value_type: ModelArg
    loop: asyncio.AbstractEventLoop = None
    is_iterator: bool

    _queue: asyncio.Queue

    def __init__(self, app: AppT,
                 *,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 is_iterator: bool = False,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.app = app
        self.loop = loop
        self.key_type = key_type
        self.value_type = value_type
        self.is_iterator = is_iterator
        self._queue = None
        self.deliver = self._compile_deliver()  # type: ignore

    def clone(self, *, is_iterator: bool = None) -> ChannelT:
        return type(self)(
            is_iterator=(is_iterator if is_iterator is not None
                         else self.is_iterator),
            **self._clone_args())

    def _clone_args(self) -> Mapping:
        return {
            'app': self.app,
            'loop': self.loop,
            'key_type': self.key_type,
            'value_type': self.value_type,
        }

    @property
    def queue(self) -> asyncio.Queue:
        # loads self.loop lazily, as channel may be created at compile-time.
        if self._queue is None:
            self._queue = asyncio.Queue(loop=self.loop or self.app.loop)
        return self._queue

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
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None,
            force: bool = False) -> FutureMessage:
        """Send message to topic."""
        if not force:
            event = current_event()
            if event is not None:
                return event.attach(
                    self, key, value,
                    partition=partition,
                    key_serializer=key_serializer,
                    value_serializer=value_serializer,
                    callback=callback,
                )
        return await self._send_now(
            key, value,
            partition=partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

    async def _send_now(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> FutureMessage:
        return await self._publish_message(FutureMessage(PendingMessage(
            self,
            await self.prepare_key(self, key, key_serializer),
            await self.prepare_value(self, value, value_serializer),
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            partition=partition,
            callback=callback,
        )))

    async def _publish_message(self, fut: FutureMessage) -> FutureMessage:
        event = self._create_event(fut.message.key, fut.message.value)
        await self.put(event)
        return await self._finalize_message(
            fut, RecordMetadata(None, None, None, None))

    async def _finalize_message(
            self, fut: FutureMessage, result: RecordMetadata) -> FutureMessage:
        fut.set_result(result)
        if fut.message.callback:
            await maybe_async(fut.message.callback(fut))
        return fut

    def send_soon(self,
                  key: K = None,
                  value: V = None,
                  partition: int = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None,
                  callback: MessageSentCallback = None) -> FutureMessage:
        """Send message to topic (asynchronous version).

        Notes:
            This can be used from non-async functions, but with the caveat
            that sending of the message will be scheduled in the event loop.
            This will buffer up the message, making backpressure a concern.
        """
        return self.app.send_soon(
            self, key, value, partition,
            key_serializer, value_serializer, callback)

    async def maybe_declare(self) -> None:
        ...

    async def declare(self) -> None:
        ...

    async def prepare_key(self,
                          topic: Union[str, ChannelT],
                          key: K,
                          key_serializer: CodecArg) -> Any:
        return key

    async def prepare_value(self,
                            topic: Union[str, ChannelT],
                            value: V,
                            value_serializer: CodecArg) -> Any:
        return value

    async def deliver(self, message: Message) -> None:
        ...  # closure compiled at __init__

    def _compile_deliver(self) -> Callable[[Message], Awaitable]:
        app = self.app
        key_type = self.key_type
        value_type = self.value_type
        loads_key = app.serializers.loads_key
        loads_value = app.serializers.loads_value
        create_event = self._create_event
        put = None

        async def deliver(message: Message) -> None:
            nonlocal put
            if put is None:
                # NOTE circumvents self.put, using queue directly
                put = self.queue.put
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
                    await put(create_event(k, v, message))
        return deliver

    def _create_event(self, key: K, value: V,
                      message: Message = None) -> EventT:
        return Event(self.app, key, value, message)

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

    def __aiter__(self) -> ChannelT:
        return self.clone(is_iterator=True)

    async def __anext__(self) -> EventT:
        if not self.is_iterator:
            raise RuntimeError('Need to call channel.__aiter__()')
        return await self.queue.get()

    def __repr__(self) -> str:
        return f'<{self.label}>'

    @property
    def label(self) -> str:
        return f'{type(self).__name__}: {self}'

    def __str__(self) -> str:
        return f'{id(self):#x}'
