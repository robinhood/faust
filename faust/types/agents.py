import abc
import asyncio
import typing
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Generic,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    no_type_check,
)

from mode import ServiceT, SupervisorStrategyT
from mode.utils.collections import ManagedUserDict

from .codecs import CodecArg
from .core import HeadersArg, K, V
from .events import EventT
from .models import ModelArg
from .streams import StreamT
from .topics import ChannelT
from .tuples import Message, RecordMetadata, TP

if typing.TYPE_CHECKING:
    from .app import AppT as _AppT
else:
    class _AppT: ...          # noqa

__all__ = [
    'AgentErrorHandler',
    'AgentFun',
    'ActorT',
    'ActorRefT',
    'AgentManagerT',
    'AgentT',
    'AgentTestWrapperT',
    'AsyncIterableActorT',
    'AwaitableActorT',
    'ReplyToArg',
    'SinkT',
]

_T = TypeVar('_T')
AgentErrorHandler = Callable[['AgentT', BaseException], Awaitable]
AgentFun = Callable[
    [StreamT],
    Union[Coroutine[Any, Any, None], Awaitable[None], AsyncIterable],
]


#: A sink can be: Agent, Channel
#: or callable/async callable taking value as argument.
SinkT = Union['AgentT', ChannelT, Callable[[Any], Union[Awaitable, None]]]

ReplyToArg = Union['AgentT', ChannelT, str]


class ActorT(ServiceT, Generic[_T]):

    agent: 'AgentT'
    stream: StreamT
    it: _T
    actor_task: Optional[asyncio.Task]
    active_partitions: Optional[Set[TP]]

    #: If multiple instance are started for concurrency, this is its index.
    index: Optional[int] = None

    @abc.abstractmethod
    def __init__(self, agent: 'AgentT', stream: StreamT, it: _T,
                 active_partitions: Set[TP] = None,
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def cancel(self) -> None:
        ...

    @abc.abstractmethod
    async def on_isolated_partition_revoked(self, tp: TP) -> None:
        ...

    @abc.abstractmethod
    async def on_isolated_partition_assigned(self, tp: TP) -> None:
        ...


class AsyncIterableActorT(ActorT[AsyncIterable], AsyncIterable):
    """Used for agent function that yields."""


class AwaitableActorT(ActorT[Awaitable], Awaitable):
    """Used for agent function that do not yield."""


ActorRefT = ActorT[Union[AsyncIterable, Awaitable]]


class AgentT(ServiceT):

    name: str
    app: _AppT
    concurrency: int
    help: str
    supervisor_strategy: Optional[Type[SupervisorStrategyT]]
    isolated_partitions: bool

    @abc.abstractmethod
    def __init__(self,
                 fun: AgentFun,
                 *,
                 name: str = None,
                 app: _AppT = None,
                 channel: Union[str, ChannelT] = None,
                 concurrency: int = 1,
                 sink: Iterable[SinkT] = None,
                 on_error: AgentErrorHandler = None,
                 supervisor_strategy: Type[SupervisorStrategyT] = None,
                 help: str = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 isolated_partitions: bool = False,
                 **kwargs: Any) -> None:
        self.fun: AgentFun = fun

    @abc.abstractmethod
    def __call__(self, *,
                 index: int = None,
                 active_partitions: Set[TP] = None,
                 stream: StreamT = None,
                 channel: ChannelT = None) -> ActorRefT:
        ...

    @abc.abstractmethod
    def test_context(self,
                     channel: ChannelT = None,
                     supervisor_strategy: SupervisorStrategyT = None,
                     **kwargs: Any) -> 'AgentTestWrapperT':
        ...

    @abc.abstractmethod
    def add_sink(self, sink: SinkT) -> None:
        ...

    @abc.abstractmethod
    def stream(self, **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        ...

    @abc.abstractmethod
    async def cast(self,
                   value: V = None,
                   *,
                   key: K = None,
                   partition: int = None,
                   timestamp: float = None,
                   headers: HeadersArg = None) -> None:
        ...

    @abc.abstractmethod
    async def ask(self,
                  value: V = None,
                  *,
                  key: K = None,
                  partition: int = None,
                  timestamp: float = None,
                  headers: HeadersArg = None,
                  reply_to: ReplyToArg = None,
                  correlation_id: str = None) -> Any:
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
                   reply_to: ReplyToArg = None,
                   correlation_id: str = None) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    @no_type_check  # XXX mypy bugs out on this
    async def map(self,
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
    async def join(self,
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

    @abc.abstractmethod
    def info(self) -> Mapping:
        ...

    @abc.abstractmethod
    def clone(self, *, cls: Type['AgentT'] = None, **kwargs: Any) -> 'AgentT':
        ...

    @abc.abstractmethod
    def get_topic_names(self) -> Iterable[str]:
        ...

    @property
    @abc.abstractmethod
    def channel(self) -> ChannelT:
        ...

    @channel.setter
    def channel(self, channel: ChannelT) -> None:
        ...

    @property
    @abc.abstractmethod
    def channel_iterator(self) -> AsyncIterator:
        ...

    @channel_iterator.setter
    def channel_iterator(self, channel: AsyncIterator) -> None:
        ...


class AgentManagerT(ServiceT, ManagedUserDict[str, AgentT]):
    app: _AppT

    @abc.abstractmethod
    async def on_rebalance(self,
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        ...


class AgentTestWrapperT(AgentT, AsyncIterable):

    new_value_processed: asyncio.Condition
    original_channel: ChannelT
    results: MutableMapping[int, Any]
    sent_offset: int = 0
    processed_offset: int = 0

    @abc.abstractmethod
    def __init__(self,
                 *args: Any,
                 original_channel: ChannelT = None,
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    async def put(self,
                  value: V = None,
                  key: K = None,
                  partition: int = None,
                  timestamp: float = None,
                  headers: HeadersArg = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None,
                  *,
                  reply_to: ReplyToArg = None,
                  correlation_id: str = None,
                  wait: bool = True) -> EventT:
        ...

    @abc.abstractmethod
    def to_message(self,
                   key: K,
                   value: V,
                   *,
                   partition: int = 0,
                   offset: int = 0,
                   timestamp: float = None,
                   timestamp_type: int = 0,
                   headers: HeadersArg = None) -> Message:
        ...

    @abc.abstractmethod
    async def throw(self, exc: BaseException) -> None:
        ...
