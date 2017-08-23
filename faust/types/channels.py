import abc
import asyncio
import typing
from types import TracebackType
from typing import Any, AsyncIterator, Type, Union
from ._coroutines import StreamCoroutine
from .codecs import CodecArg
from .core import K, V
from .tuples import FutureMessage, Message, MessageSentCallback

if typing.TYPE_CHECKING:
    from .app import AppT
    from .models import ModelArg
    from .streams import StreamT
    from .transports import ConsumerT, TPorTopicSet
else:
    class AppT: ...             # noqa
    class ModelArg: ...         # noqa
    class StreamT: ...          # noqa
    class ConsumerT: ...        # noqa
    class TPorTopicSet: ...     # noqa


class EventT(metaclass=abc.ABCMeta):
    app: AppT
    key: K
    value: V
    message: Message

    __slots__ = ('app', 'key', 'value', 'message', '__weakref__')

    @abc.abstractmethod
    def __init__(self, app: AppT, key: K, value: V, message: Message) -> None:
        ...

    @abc.abstractmethod
    async def send(self, topic: Union[str, 'ChannelT'],
                   *,
                   key: Any = None,
                   force: bool = False) -> FutureMessage:
        ...

    @abc.abstractmethod
    async def forward(self, topic: Union[str, 'ChannelT'],
                      *,
                      key: Any = None,
                      force: bool = False) -> FutureMessage:
        ...

    @abc.abstractmethod
    def attach(self,
               channel: Union['ChannelT', str],
               key: K = None,
               value: V = None,
               partition: int = None,
               key_serializer: CodecArg = None,
               value_serializer: CodecArg = None,
               callback: MessageSentCallback = None) -> FutureMessage:
        ...

    @abc.abstractmethod
    async def ack(self) -> None:
        ...

    @abc.abstractmethod
    async def __aenter__(self) -> 'EventT':
        ...

    @abc.abstractmethod
    async def __aexit__(self,
                        exc_type: Type[Exception],
                        exc_val: Exception,
                        exc_tb: TracebackType) -> None:
        ...


class ChannelT(AsyncIterator):
    app: AppT
    key_type: ModelArg
    value_type: ModelArg
    loop: asyncio.AbstractEventLoop = None

    @abc.abstractmethod
    def __init__(self, app: AppT,
                 *,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    @abc.abstractmethod
    def clone(self, *, is_iterator: bool = None) -> 'ChannelT':
        ...

    @abc.abstractmethod
    def stream(self, coroutine: StreamCoroutine = None,
               **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    async def send(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None,
            force: bool = False) -> FutureMessage:
        ...

    @abc.abstractmethod
    def send_soon(self, key: K, value: V,
                  partition: int = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None,
                  callback: MessageSentCallback = None) -> FutureMessage:
        ...

    @abc.abstractmethod
    async def maybe_declare(self) -> None:
        ...

    @abc.abstractmethod
    async def declare(self) -> None:
        ...

    @abc.abstractmethod
    async def prepare_key(self,
                          topic: Union[str, 'ChannelT'],
                          key: K,
                          key_serializer: CodecArg) -> Any:
        ...

    @abc.abstractmethod
    async def prepare_value(self,
                            topic: Union[str, 'ChannelT'],
                            value: V,
                            value_serializer: CodecArg) -> Any:
        ...

    @abc.abstractmethod
    async def deliver(self, message: Message) -> None:
        ...

    @abc.abstractmethod
    async def put(self, value: Any) -> None:
        ...

    @abc.abstractmethod
    async def get(self) -> Any:
        ...

    @abc.abstractmethod
    def __aiter__(self) -> 'ChannelT':
        ...

    @abc.abstractmethod
    async def __anext__(self) -> EventT:
        ...
