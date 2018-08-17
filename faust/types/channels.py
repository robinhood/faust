import abc
import asyncio
import typing
from typing import Any, AsyncIterator, Awaitable, Optional, Set

from mode import Seconds
from mode.utils.futures import stampede
from mode.utils.queues import ThrowableQueue

from .codecs import CodecArg
from .core import K, V
from .tuples import (
    FutureMessage,
    Message,
    MessageSentCallback,
    RecordMetadata,
    TP,
)

if typing.TYPE_CHECKING:
    from .app import AppT
    from .events import EventT
    from .models import ModelArg
    from .streams import StreamT
    from .transports import ConsumerT, TPorTopicSet
else:
    class AppT: ...             # noqa
    class EventT: ...           # noqa
    class ModelArg: ...         # noqa
    class StreamT: ...          # noqa
    class ConsumerT: ...        # noqa
    class TPorTopicSet: ...     # noqa


class ChannelT(AsyncIterator):
    app: AppT
    key_type: Optional[ModelArg]
    value_type: Optional[ModelArg]
    loop: Optional[asyncio.AbstractEventLoop]
    maxsize: Optional[int]
    active_partitions: Optional[Set[TP]]

    @abc.abstractmethod
    def __init__(self,
                 app: AppT,
                 *,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 is_iterator: bool = False,
                 queue: ThrowableQueue = None,
                 maxsize: int = None,
                 root: 'ChannelT' = None,
                 active_partitions: Set[TP] = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    @abc.abstractmethod
    def clone(self, *, is_iterator: bool = None, **kwargs: Any) -> 'ChannelT':
        ...

    @abc.abstractmethod
    def stream(self, **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    def get_topic_name(self) -> str:
        ...

    @abc.abstractmethod
    async def send(self,
                   *,
                   key: K = None,
                   value: V = None,
                   partition: int = None,
                   key_serializer: CodecArg = None,
                   value_serializer: CodecArg = None,
                   callback: MessageSentCallback = None,
                   force: bool = False) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    def as_future_message(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> FutureMessage:
        ...

    @abc.abstractmethod
    async def publish_message(self, fut: FutureMessage,
                              wait: bool = True) -> Awaitable[RecordMetadata]:
        ...

    @stampede
    @abc.abstractmethod
    async def maybe_declare(self) -> None:
        ...

    @abc.abstractmethod
    async def declare(self) -> None:
        ...

    @abc.abstractmethod
    def prepare_key(self, key: K, key_serializer: CodecArg) -> Any:
        ...

    @abc.abstractmethod
    def prepare_value(self, value: V, value_serializer: CodecArg) -> Any:
        ...

    @abc.abstractmethod
    async def decode(self, message: Message, *,
                     propagate: bool = False) -> EventT:
        ...

    @abc.abstractmethod
    async def deliver(self, message: Message) -> None:
        ...

    @abc.abstractmethod
    async def put(self, value: Any) -> None:
        ...

    @abc.abstractmethod
    async def get(self, *, timeout: Seconds = None) -> Any:
        ...

    @abc.abstractmethod
    def empty(self) -> bool:
        ...

    @abc.abstractmethod
    async def on_key_decode_error(self, exc: Exception,
                                  message: Message) -> None:
        ...

    @abc.abstractmethod
    async def on_value_decode_error(self, exc: Exception,
                                    message: Message) -> None:
        ...

    @abc.abstractmethod
    async def on_decode_error(self, exc: Exception, message: Message) -> None:
        ...

    @abc.abstractmethod
    def on_stop_iteration(self) -> None:
        ...

    @abc.abstractmethod
    def __aiter__(self) -> 'ChannelT':
        ...

    @abc.abstractmethod
    async def __anext__(self) -> EventT:
        ...

    @abc.abstractmethod
    async def throw(self, exc: BaseException) -> None:
        ...

    @abc.abstractmethod
    def derive(self, **kwargs: Any) -> 'ChannelT':
        ...

    @property
    @abc.abstractmethod
    def subscriber_count(self) -> int:
        ...

    @property
    @abc.abstractmethod
    def queue(self) -> ThrowableQueue:
        ...
