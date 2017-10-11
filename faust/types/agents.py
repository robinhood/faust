import abc
import asyncio
import typing
from typing import (
    Any, AsyncIterable, AsyncIterator, Awaitable, Callable,
    Generic, Iterable, List, Tuple, TypeVar, Union, no_type_check,
)
from mode import ServiceT
from .codecs import CodecArg
from .core import K, V
from .streams import StreamT
from .topics import ChannelT
from .tuples import RecordMetadata

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...          # noqa

__all__ = [
    'AgentErrorHandler',
    'AgentFun',
    'ActorT',
    'ActorRefT',
    'AgentT',
    'AsyncIterableActorT',
    'AwaitableActorT',
    'ReplyToArg',
    'SinkT',
]

_T = TypeVar('_T')
AgentErrorHandler = Callable[['AgentT', BaseException], Awaitable]
AgentFun = Callable[
    [Union[AsyncIterator, StreamT]],
    Union[Awaitable, AsyncIterable],
]

#: A sink can be: Agent, Channel
#: or callable/async callable taking value as argument.
SinkT = Union['AgentT', ChannelT, Callable[[Any], Union[Awaitable, None]]]

ReplyToArg = Union['AgentT', ChannelT, str]


class ActorT(Generic[_T], ServiceT):

    agent: 'AgentT'
    stream: StreamT
    it: _T
    actor_task: asyncio.Task = None

    #: If multiple instance are started for concurrency, this is its index.
    index: int = None

    @abc.abstractmethod
    def __init__(self,
                 agent: 'AgentT',
                 stream: StreamT,
                 it: _T,
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def cancel(self) -> None:
        ...


class AsyncIterableActorT(ActorT[AsyncIterable], AsyncIterable):
    """Used for agent function that yields."""
    ...


class AwaitableActorT(ActorT[Awaitable], Awaitable):
    """Used for agent function that do not yield."""
    ...


ActorRefT = ActorT[Union[AsyncIterable, Awaitable]]


class AgentT(ServiceT):

    name: str
    app: AppT
    channel: ChannelT
    concurrency: int
    help: str

    @abc.abstractmethod
    def __init__(self, fun: AgentFun,
                 *,
                 name: str = None,
                 app: AppT = None,
                 channel: Union[str, ChannelT] = None,
                 concurrency: int = 1,
                 sink: Iterable[SinkT] = None,
                 on_error: AgentErrorHandler = None,
                 help: str = None) -> None:
        self.fun: AgentFun = fun

    @abc.abstractmethod
    def __call__(self) -> ActorRefT:
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
