import abc
import asyncio
import typing
from typing import (
    Any, AsyncIterator, Awaitable, Callable,
    List, Mapping, Sequence, Tuple, Type, TypeVar, Union,
)
from ..utils.types.services import ServiceT
from ._coroutines import StreamCoroutine
from .core import K
from .models import Event, FieldDescriptorT
from .topics import TopicT

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
    'GroupByKeyArg',
    'StreamCoroutine',
    'StreamT',
]

# Used for typing StreamT[Withdrawal]
_T = TypeVar('_T')

Processor = Callable[[Event], Union[Event, Awaitable[Event]]]


#: Type of the `key` argument to `Stream.group_by()`
GroupByKeyArg = Union[
    FieldDescriptorT,
    Callable[[Event], K],
]


class JoinableT(abc.ABC):

    @abc.abstractmethod
    def combine(self, *nodes: 'JoinableT', **kwargs: Any) -> 'StreamT':
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
    def __and__(self, other: 'JoinableT') -> 'StreamT':
        ...


class StreamT(AsyncIterator[_T], JoinableT, ServiceT):

    app: AppT = None
    source: AsyncIterator = None
    name: str = None
    outbox: asyncio.Queue = None
    join_strategy: JoinT = None
    task_owner: asyncio.Task = None

    children: List[JoinableT] = None

    @abc.abstractmethod
    def __init__(self, app: AppT,
                 *,
                 name: str = None,
                 source: AsyncIterator = None,
                 processors: Sequence[Processor] = None,
                 coroutine: StreamCoroutine = None,
                 children: List[JoinableT] = None,
                 join_strategy: JoinT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    @abc.abstractmethod
    def add_processor(self, processor: Processor) -> None:
        ...

    @abc.abstractmethod
    def asdict(self) -> Mapping[str, Any]:
        ...

    @abc.abstractmethod
    def clone(self, **kwargs: Any) -> 'StreamT':
        ...

    @abc.abstractmethod
    async def items(self) -> AsyncIterator[Tuple[K, Event]]:
        ...

    @abc.abstractmethod
    async def take(self, max_events: int,
                   within: float = None) -> AsyncIterator[Sequence[Event]]:
        ...

    @abc.abstractmethod
    def tee(self, n: int = 2) -> Tuple['StreamT', ...]:
        ...

    @abc.abstractmethod
    def through(self, topic: Union[str, TopicT]) -> 'StreamT':
        ...

    @abc.abstractmethod
    def group_by(self, key: GroupByKeyArg) -> 'StreamT':
        ...

    @abc.abstractmethod
    def derive_topic(self, name: str,
                     *,
                     key_type: Type = None,
                     value_type: Type = None,
                     prefix: str = '',
                     suffix: str = '') -> TopicT:
        ...

    @abc.abstractmethod
    def enumerate(self,
                  start: int = 0) -> AsyncIterator[Tuple[int, Event]]:
        ...

    @abc.abstractmethod
    async def send(self, value: Event) -> None:
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
