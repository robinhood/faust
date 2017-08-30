import abc
import typing
from typing import (
    Any, AsyncIterable, AsyncIterator, Awaitable,
    Callable, Iterable, List, Tuple, Union, no_type_check,
)
from .codecs import CodecArg
from .core import K, V
from .streams import StreamT
from .topics import ChannelT
from .tuples import RecordMetadata
from ..utils.types.services import ServiceT

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...          # noqa

__all__ = [
    'ActorErrorHandler',
    'ActorFun',
    'ActorT',
    'ReplyToArg',
]

ActorErrorHandler = Callable[['ActorT', Exception], Awaitable]
ActorFun = Callable[
    [Union[AsyncIterator, StreamT]],
    Union[Awaitable, AsyncIterable],
]

#: A sink can be: Actor, Channel
#: or callable/async callable taking value as argument.
SinkT = Union['ActorT', ChannelT, Callable[[Any], Union[Awaitable, None]]]

ReplyToArg = Union['ActorT', ChannelT, str]


class ActorT(ServiceT):

    name: str
    app: AppT
    channel: ChannelT
    concurrency: int

    @abc.abstractmethod
    def __init__(self, fun: ActorFun,
                 *,
                 name: str = None,
                 app: AppT = None,
                 channel: Union[str, ChannelT] = None,
                 concurrency: int = 1,
                 sink: Iterable[SinkT] = None,
                 on_error: ActorErrorHandler = None) -> None:
        self.fun: ActorFun = fun

    @abc.abstractmethod
    def __call__(self) -> Union[Awaitable, AsyncIterable]:
        ...

    @abc.abstractmethod
    def add_sink(self, sink: SinkT) -> None:
        ...

    @abc.abstractmethod
    def stream(self, **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    async def cast(
            self,
            value: V = None,
            *,
            key: K = None,
            partition: int = None) -> None:
        ...

    @abc.abstractmethod
    async def ask(
            self,
            value: V = None,
            *,
            key: K = None,
            partition: int = None,
            reply_to: ReplyToArg = None,
            correlation_id: str = None) -> Any:
        ...

    @abc.abstractmethod
    async def send(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            *,
            reply_to: ReplyToArg = None,
            correlation_id: str = None) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    def send_soon(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    @no_type_check  # XXX mypy bugs out on this
    async def map(
            self,
            values: Union[AsyncIterable, Iterable],
            key: K = None,
            reply_to: ReplyToArg = None) -> AsyncIterator:
        ...

    @abc.abstractmethod
    @no_type_check  # XXX mypy bugs out on this
    async def kvmap(
            self,
            items: Union[AsyncIterable[Tuple[K, V]], Iterable[Tuple[K, V]]],
            reply_to: ReplyToArg = None) -> AsyncIterator[str]:
        ...

    @abc.abstractmethod
    async def join(
            self,
            values: Union[AsyncIterable[V], Iterable[V]],
            key: K = None,
            reply_to: ReplyToArg = None) -> List[Any]:
        ...

    @abc.abstractmethod
    async def kvjoin(
            self,
            items: Union[AsyncIterable[Tuple[K, V]], Iterable[Tuple[K, V]]],
            reply_to: ReplyToArg = None) -> List[Any]:
        ...

    @property
    @abc.abstractmethod
    def channel_iterator(self) -> AsyncIterator:
        ...

    @channel_iterator.setter
    def channel_iterator(self, channel: AsyncIterator) -> None:
        ...
