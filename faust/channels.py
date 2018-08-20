"""Channel.

A channel is used to send values to streams.

The stream will iterate over incoming events in the channel.

"""
import asyncio
import typing
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
from .types.tuples import _PendingMessage_to_Message

if typing.TYPE_CHECKING:  # pragma: no cover
    from .app.base import App
else:
    class App: ...  # noqa

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
        loop: The asyncio event loop to use.
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
        subchannel: Channel = type(self)(
            is_iterator=(is_iterator
                         if is_iterator is not None else self.is_iterator),
            **{**self._clone_args(), **kwargs})
        (self._root or self)._subscribers.add(subchannel)
        # make sure queue is created at this point
        # ^ it's a cached_property
        subchannel.queue
        return subchannel

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
        raise NotImplementedError('Channels are unnamed topics')

    async def send(self,
                   *,
                   key: K = None,
                   value: V = None,
                   partition: int = None,
                   key_serializer: CodecArg = None,
                   value_serializer: CodecArg = None,
                   callback: MessageSentCallback = None,
                   force: bool = False) -> Awaitable[RecordMetadata]:
        """Send message to channel."""
        return await self._send_now(
            key,
            value,
            partition=partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

    def as_future_message(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> FutureMessage:
        return FutureMessage(
            PendingMessage(
                self,
                self.prepare_key(key, key_serializer),
                self.prepare_value(value, value_serializer),
                key_serializer=key_serializer,
                value_serializer=value_serializer,
                partition=partition,
                callback=callback,
                # Python 3.6.0: NamedTuple doesn't support optional fields
                # [ask]
                topic=None,
                offset=None,
            ),
        )

    async def _send_now(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        return await self.publish_message(
            self.as_future_message(key, value, partition, key_serializer,
                                   value_serializer, callback))

    async def publish_message(self, fut: FutureMessage,
                              wait: bool = True) -> Awaitable[RecordMetadata]:
        event = self._create_event(
            fut.message.key, fut.message.value,
            message=_PendingMessage_to_Message(fut.message))
        await self.put(event)
        return await self._finalize_message(
            fut, RecordMetadata('topic', -1, TP('topic', -1), -1))

    async def _finalize_message(self, fut: FutureMessage,
                                result: RecordMetadata) -> FutureMessage:
        fut.set_result(result)
        if fut.message.callback:
            await maybe_async(fut.message.callback(fut))
        return fut

    @stampede
    async def maybe_declare(self) -> None:
        ...

    async def declare(self) -> None:
        ...

    def prepare_key(self, key: K, key_serializer: CodecArg) -> Any:
        return key

    def prepare_value(self, value: V, value_serializer: CodecArg) -> Any:
        return value

    async def decode(self, message: Message, *,
                     propagate: bool = False) -> EventT:
        return self._create_event(message.key, message.value, message=message)

    async def deliver(self, message: Message) -> None:  # pragma: no cover
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

    def _create_event(self, key: K, value: V, message: Message) -> EventT:
        return Event(self.app, key, value, message)

    async def put(self, value: Any) -> None:
        root = self._root if self._root is not None else self
        for subscriber in root._subscribers:
            await subscriber.queue.put(value)

    async def get(self, *, timeout: Seconds = None) -> Any:
        timeout_: float = want_seconds(timeout)
        if timeout_:
            return await asyncio.wait_for(self.queue.get(), timeout=timeout_)
        return await self.queue.get()

    def empty(self) -> bool:
        return self.queue.empty()

    async def on_key_decode_error(self, exc: Exception,
                                  message: Message) -> None:
        await self.on_decode_error(exc, message)
        await self.throw(exc)

    async def on_value_decode_error(self, exc: Exception,
                                    message: Message) -> None:
        await self.on_decode_error(exc, message)
        await self.throw(exc)

    async def on_decode_error(self, exc: Exception, message: Message) -> None:
        ...

    def on_stop_iteration(self) -> None:
        ...

    def derive(self, **kwargs: Any) -> ChannelT:
        return self

    def __aiter__(self) -> ChannelT:
        return self if self.is_iterator else self.clone(is_iterator=True)

    async def __anext__(self) -> EventT:
        if not self.is_iterator:
            raise RuntimeError('Need to call channel.__aiter__()')
        return await self.queue.get()

    async def throw(self, exc: BaseException) -> None:
        await self.queue.throw(exc)

    def __repr__(self) -> str:
        s = f'<{self.label}@{self._repr_id()}'
        if self.active_partitions is not None:
            if self.active_partitions:
                active = '{' + ', '.join(sorted(
                    f'{tp.topic}:{tp.partition}'
                    for tp in self.active_partitions)) + '}'
            else:
                active = '{<pending for assignment>}'
            s += f' active={active}'
        s += '>'
        return s

    def __str__(self) -> str:
        return '<ANON>'

    def _repr_id(self) -> str:
        return f'{id(self):#x}'

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    @property
    def label(self) -> str:
        sym = '(*)' if self.is_iterator else ''
        return f'{sym}{type(self).__name__}: {self}'
