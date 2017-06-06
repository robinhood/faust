import abc
import asyncio
import typing
from typing import (
    Any, AsyncIterator, Awaitable, Callable, Iterable,
    List, Mapping, Sequence, Tuple, TypeVar, Union,
)
from ..utils.times import Seconds
from ..utils.types.services import ServiceT
from ._coroutines import StreamCoroutine
from .core import K
from .models import FieldDescriptorT, ModelArg
from .topics import EventT, TopicT

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
    '_T',
]

# Used for typing StreamT[Withdrawal]
_T = TypeVar('_T')

Processor = Callable[[_T], Union[_T, Awaitable[_T]]]


#: Type of the `key` argument to `Stream.group_by()`
GroupByKeyArg = Union[
    FieldDescriptorT,
    Callable[[_T], K],
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

    source: AsyncIterator[_T] = None
    outbox: asyncio.Queue = None
    join_strategy: JoinT = None
    task_owner: asyncio.Task = None

    children: List[JoinableT] = None

    @abc.abstractmethod
    def __init__(self, source: AsyncIterator[_T] = None,
                 *,
                 processors: Iterable[Processor] = None,
                 coroutine: StreamCoroutine = None,
                 children: List[JoinableT] = None,
                 join_strategy: JoinT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    @abc.abstractmethod
    def add_processor(self, processor: Processor) -> None:
        ...

    @abc.abstractmethod
    def info(self) -> Mapping[str, Any]:
        ...

    @abc.abstractmethod
    def clone(self, **kwargs: Any) -> 'StreamT':
        ...

    @abc.abstractmethod
    async def items(self) -> AsyncIterator[Tuple[K, _T]]:
        ...

    @abc.abstractmethod
    async def events(self) -> AsyncIterator[EventT]:
        ...

    @abc.abstractmethod
    async def take(self, max_: int,
                   within: Seconds = None) -> AsyncIterator[Sequence[_T]]:
        ...

    @abc.abstractmethod
    def tee(self, n: int = 2) -> Tuple['StreamT', ...]:
        ...

    @abc.abstractmethod
    def enumerate(self,
                  start: int = 0) -> AsyncIterator[Tuple[int, _T]]:
        ...

    @abc.abstractmethod
    def through(self, topic: Union[str, TopicT]) -> 'StreamT':
        ...

    @abc.abstractmethod
    def echo(self, *topics: Union[str, TopicT]) -> 'StreamT':
        ...

    @abc.abstractmethod
    def group_by(self, key: GroupByKeyArg,
                 *,
                 name: str = None,
                 topic: TopicT = None) -> 'StreamT':
        ...

    @abc.abstractmethod
    def derive_topic(self, name: str,
                     *,
                     key_type: ModelArg = None,
                     value_type: ModelArg = None,
                     prefix: str = '',
                     suffix: str = '') -> TopicT:
        ...

    @abc.abstractmethod
    async def send(self, value: _T) -> None:
        ...

    @abc.abstractmethod
    def __copy__(self) -> 'StreamT':
        ...

    @abc.abstractmethod
    def __iter__(self) -> Any:
        ...

    @abc.abstractmethod
    def __next__(self) -> _T:
        ...

    @abc.abstractmethod
    def __aiter__(self) -> AsyncIterator[_T]:
        ...

    @abc.abstractmethod
    async def __anext__(self) -> _T:
        ...
