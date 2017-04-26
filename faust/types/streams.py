import abc
import asyncio
import typing
from typing import (
    Any, AsyncIterator, Awaitable, Callable,
    List, Mapping, MutableMapping, MutableSequence,
    Sequence, Tuple, Type, TypeVar, Union,
)
from ..utils.types.services import ServiceT
from ._coroutines import CoroCallbackT, StreamCoroutine
from .core import K
from .models import Event, FieldDescriptorT
from .transports import ConsumerT
from .tuples import Message, Topic, TopicPartition
from .windows import WindowT

if typing.TYPE_CHECKING:
    from .app import AppT
    from .join import JoinT
    from .tables import TableT
else:
    class AppT: ...    # noqa
    class JoinT: ...   # noqa
    class TableT: ...  # noqa

__all__ = [
    'Processor',
    'TopicProcessorSequence',
    'StreamProcessorMap',
    'StreamCoroutineMap',
    'GroupByKeyArg',
    'StreamT',
    'StreamManagerT',
]

# Used for typing StreamT[Withdrawal]
_T = TypeVar('_T')

Processor = Callable[[Event], Union[Event, Awaitable[Event]]]
TopicProcessorSequence = Sequence[Processor]
StreamProcessorMap = MutableMapping[Topic, TopicProcessorSequence]
StreamCoroutineMap = MutableMapping[Topic, CoroCallbackT]


GroupByKeyArg = Union[
    FieldDescriptorT,
    Callable[[Event], K],
]


class StreamT(AsyncIterator[_T], ServiceT):

    active: bool = True
    app: AppT = None
    topics: MutableSequence[Topic] = None
    name: str = None
    outbox: asyncio.Queue = None
    join_strategy: JoinT = None

    children: List['StreamT'] = None

    @classmethod
    @abc.abstractmethod
    def from_topic(cls, topic: Topic = None,
                   *,
                   coroutine: StreamCoroutine = None,
                   processors: TopicProcessorSequence = None,
                   loop: asyncio.AbstractEventLoop = None,
                   **kwargs: Any) -> 'StreamT':
        ...

    def __init__(self, name: str = None,
                 topics: Sequence[Topic] = None,
                 processors: StreamProcessorMap = None,
                 coroutines: StreamCoroutineMap = None,
                 children: List['StreamT'] = None,
                 join_strategy: JoinT = None,
                 app: AppT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    @abc.abstractmethod
    def bind(self, app: AppT) -> 'StreamT':
        ...

    @abc.abstractmethod
    def _bind(self, app: AppT) -> 'StreamT':
        ...

    @abc.abstractmethod
    def add_processor(self, processor: Processor,
                      *,
                      topics: Sequence[Topic] = None) -> None:
        ...

    @abc.abstractmethod
    def info(self) -> Mapping[str, Any]:
        ...

    @abc.abstractmethod
    def clone(self, **kwargs: Any) -> 'StreamT':
        ...

    @abc.abstractmethod
    def combine(self, *nodes: 'StreamT', **kwargs: Any) -> 'StreamT':
        ...

    @abc.abstractmethod
    async def items(self) -> AsyncIterator[Tuple[K, Event]]:
        ...

    @abc.abstractmethod
    def tee(self, n: int = 2) -> Tuple['StreamT', ...]:
        ...

    @abc.abstractmethod
    def through(self, topic: Union[str, Topic]) -> 'StreamT':
        ...

    @abc.abstractmethod
    def group_by(self, key: GroupByKeyArg) -> 'StreamT':
        ...

    @abc.abstractmethod
    def aggregate(self, table_name: str,
                  operator: Callable[[Any, Event], Any],
                  *,
                  window: WindowT = None,
                  default: Callable[[], Any] = None,
                  key: FieldDescriptorT = None,
                  key_type: Type = None,
                  value_type: Type = None) -> TableT:
        ...

    @abc.abstractmethod
    def count(self, table_name: str,
              *,
              key: FieldDescriptorT = None,
              **kwargs: Any) -> TableT:
        ...

    @abc.abstractmethod
    def sum(self, field: FieldDescriptorT, table_name: str,
            *,
            default: Callable[[], Any] = int,
            key: FieldDescriptorT,
            value_type: Type = None,
            **kwargs: Any) -> TableT:
        ...

    @abc.abstractmethod
    def derive_topic(self, name: str) -> Topic:
        ...

    @abc.abstractmethod
    def enumerate(self,
                  start: int = 0) -> AsyncIterator[Tuple[int, Event]]:
        ...

    @abc.abstractmethod
    def join(self, *fields: FieldDescriptorT) -> 'StreamT':
        ...

    @abc.abstractmethod
    def left_join(self, *fields: FieldDescriptorT) -> 'StreamT':
        ...

    @abc.abstractmethod
    def inner_join(self, *fields: FieldDescriptorT) -> 'StreamT':
        ...

    @abc.abstractmethod
    def outer_join(self, *fields: FieldDescriptorT) -> 'StreamT':
        ...

    @abc.abstractmethod
    async def put_event(self, value: Event) -> None:
        ...

    @abc.abstractmethod
    async def process(self, key: K, value: Event) -> Event:
        ...

    @abc.abstractmethod
    async def on_done(self, value: Event = None) -> None:
        ...

    @abc.abstractmethod
    async def subscribe(self, topic: Topic,
                        *,
                        processors: TopicProcessorSequence = None,
                        coroutine: StreamCoroutine = None) -> None:
        ...

    @abc.abstractmethod
    async def unsubscribe(self, topic: Topic) -> None:
        ...

    @abc.abstractmethod
    async def on_key_decode_error(
            self, exc: Exception, message: Message) -> None:
        ...

    @abc.abstractmethod
    async def on_value_decode_error(
            self, exc: Exception, message: Message) -> None:
        ...

    @abc.abstractmethod
    def __and__(self, other: 'StreamT') -> 'StreamT':
        ...

    @abc.abstractmethod
    def __copy__(self) -> 'StreamT':
        ...

    @abc.abstractmethod
    def __iter__(self) -> Any:
        ...

    @abc.abstractmethod
    def __next__(self) -> Event:
        ...

    @abc.abstractmethod
    def __aiter__(self) -> AsyncIterator:
        ...

    @abc.abstractmethod
    async def __anext__(self) -> Any:
        ...


class StreamManagerT(ServiceT):

    consumer: ConsumerT

    @abc.abstractmethod
    def add_stream(self, stream: StreamT) -> None:
        ...

    @abc.abstractmethod
    async def update(self) -> None:
        ...

    @abc.abstractmethod
    def ack_message(self, message: Message) -> None:
        ...

    @abc.abstractmethod
    def ack_offset(self, tp: TopicPartition, offset: int) -> None:
        ...
