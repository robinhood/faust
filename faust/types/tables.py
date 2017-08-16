import abc
import asyncio
import typing
from typing import (
    Any, Callable, ClassVar, Iterable, MutableMapping, MutableSet, Type,
)
from .stores import StoreT
from .streams import JoinableT
from .topics import EventT, TopicT
from .tuples import TopicPartition
from .windows import WindowT
from ..utils.times import Seconds
from ..utils.types.services import ServiceT

if typing.TYPE_CHECKING:
    from .app import AppT
    from .models import ModelArg
else:
    class AppT: ...      # noqa
    class ModelArg: ...  # noqa

__all__ = [
    'CollectionT',
    'TableT',
    'SetT',
    'TableManagerT',
    'WindowSetT',
    'WindowWrapperT',
]


class CollectionT(JoinableT, ServiceT):
    StateStore: ClassVar[Type[StoreT]] = None

    app: AppT
    name: str
    default: Any  # noqa: E704
    key_type: ModelArg
    value_type: ModelArg
    partitions: int
    window: WindowT = None

    @abc.abstractmethod
    def __init__(self, app: AppT,
                 *,
                 name: str = None,
                 default: Callable[[], Any] = None,
                 store: str = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 partitions: int = None,
                 window: WindowT = None,
                 changelog_topic: TopicT = None,
                 **kwargs: Any) -> None:
        ...

    @property
    @abc.abstractmethod
    def changelog_topic(self) -> TopicT:
        ...

    @changelog_topic.setter
    def changelog_topic(self, topic: TopicT) -> None:
        ...


class TableT(CollectionT, MutableMapping):

    @abc.abstractmethod
    def using_window(self, window: WindowT) -> 'WindowWrapperT':
        ...

    @abc.abstractmethod
    def hopping(self, size: Seconds, step: Seconds,
                expires: Seconds = None) -> 'WindowWrapperT':
        ...

    @abc.abstractmethod
    def tumbling(self, size: Seconds,
                 expires: Seconds = None) -> 'WindowWrapperT':
        ...


class SetT(CollectionT, MutableSet):
    ...


class StandbyT(ServiceT):
    table: TableT
    table_manager: TableManagerT
    app: AppT
    tps: Iterable[TopicPartition]
    offsets: MutableMapping[TopicPartition, int]


class TableManagerT(ServiceT, MutableMapping[str, CollectionT]):
    app: AppT
    recovery_completed: asyncio.Event

    @abc.abstractmethod
    def __init__(self, app: AppT, **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_assigned(
            self, assigned: Iterable[TopicPartition]) -> None:
        ...


class WindowSetT(MutableMapping):
    key: Any
    table: TableT
    event: EventT = None

    @abc.abstractmethod
    def __init__(self,
                 key: Any,
                 table: TableT,
                 event: EventT = None) -> None:
        ...

    @abc.abstractmethod
    def apply(self,
              op: Callable[[Any, Any], Any],
              value: Any,
              event: EventT = None) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def now(self) -> Any:
        ...

    @abc.abstractmethod
    def current(self, event: EventT = None) -> Any:
        ...

    @abc.abstractmethod
    def delta(self, d: Seconds, event: EventT = None) -> Any:
        ...

    @abc.abstractmethod
    def __iadd__(self, other: Any) -> Any:
        ...

    @abc.abstractmethod
    def __isub__(self, other: Any) -> Any:
        ...

    @abc.abstractmethod
    def __imul__(self, other: Any) -> Any:
        ...

    @abc.abstractmethod
    def __itruediv__(self, other: Any) -> Any:
        ...

    @abc.abstractmethod
    def __ifloordiv__(self, other: Any) -> Any:
        ...

    @abc.abstractmethod
    def __imod__(self, other: Any) -> Any:
        ...

    @abc.abstractmethod
    def __ipow__(self, other: Any) -> Any:
        ...

    @abc.abstractmethod
    def __ilshift__(self, other: Any) -> Any:
        ...

    @abc.abstractmethod
    def __irshift__(self, other: Any) -> Any:
        ...

    @abc.abstractmethod
    def __iand__(self, other: Any) -> Any:
        ...

    @abc.abstractmethod
    def __ixor__(self, other: Any) -> Any:
        ...

    @abc.abstractmethod
    def __ior__(self, other: Any) -> Any:
        ...


class WindowWrapperT(MutableMapping):
    table: TableT

    @abc.abstractmethod
    def __init__(self, table: TableT) -> None:
        ...

    @abc.abstractmethod
    def __getitem__(self, key: Any) -> WindowSetT:
        ...
