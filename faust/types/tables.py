import abc
import typing
from typing import Any, Callable, ClassVar, Mapping, MutableMapping, Type
from ..utils.times import Seconds
from ..utils.types.services import ServiceT
from .streams import JoinableT
from .topics import EventT, TopicT
from .windows import WindowT

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa


class TableT(MutableMapping, JoinableT, ServiceT):
    StateStore: ClassVar[Type] = None

    app: AppT
    table_name: str
    default: Any  # noqa: E704
    key_type: Type
    value_type: Type
    changelog_topic: TopicT
    partitions: int

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


class WindowSetT(MutableMapping):
    key: Any
    table: TableT
    window: WindowT
    event: EventT = None

    @abc.abstractmethod
    def __init__(self,
                 key: Any,
                 table: TableT,
                 window: WindowT,
                 event: EventT = None) -> None:
        ...

    @abc.abstractmethod
    def apply(self,
              op: Callable[[Any, Any], Any],
              value: Any,
              event: EventT = None) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def timestamp(self, event: EventT = None) -> float:
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


class WindowWrapperT(Mapping):
    table: TableT
    window: WindowT

    @abc.abstractmethod
    def __init__(self, table: TableT, window: WindowT) -> None:
        ...

    @abc.abstractmethod
    def __getitem__(self, key: Any) -> WindowSetT:
        ...

    def __setitem__(self, key, value) -> None:
        pass
