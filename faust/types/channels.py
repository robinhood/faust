import abc
import asyncio
import typing
from typing import Any, AsyncIterator, Awaitable, Optional, Set

from mode import Seconds
from mode.utils.futures import stampede
from mode.utils.queues import ThrowableQueue

from .codecs import CodecArg
from .core import HeadersArg, K, V
from .tuples import (
    FutureMessage,
    Message,
    MessageSentCallback,
    RecordMetadata,
    TP,
)

if typing.TYPE_CHECKING:
    from .app import AppT as _AppT
    from .events import EventT as _EventT
    from .models import ModelArg as _ModelArg
    from .streams import StreamT as _StreamT
else:
    class _AppT: ...             # noqa
    class _EventT: ...           # noqa
    class _ModelArg: ...         # noqa
    class _StreamT: ...          # noqa


class ChannelT(AsyncIterator):
    app: _AppT
    key_type: Optional[_ModelArg]
    value_type: Optional[_ModelArg]
    loop: Optional[asyncio.AbstractEventLoop]
    maxsize: Optional[int]
    active_partitions: Optional[Set[TP]]

    @abc.abstractmethod
    def __init__(self,
                 app: _AppT,
                 *,
                 key_type: _ModelArg = None,
                 value_type: _ModelArg = None,
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
    def clone_using_queue(self, queue: asyncio.Queue) -> 'ChannelT':
        ...

    @abc.abstractmethod
    def stream(self, **kwargs: Any) -> _StreamT:
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
                   timestamp: float = None,
                   headers: HeadersArg = None,
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
            timestamp: float = None,
            headers: HeadersArg = None,
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
                     propagate: bool = False) -> _EventT:
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
    async def __anext__(self) -> _EventT:
        ...

    @abc.abstractmethod
    async def throw(self, exc: BaseException) -> None:
        ...

    @abc.abstractmethod
    def _throw(self, exc: BaseException) -> None:
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
