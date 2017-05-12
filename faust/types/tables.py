import abc
import typing
from datetime import timedelta
from typing import Any, Callable, ClassVar, Mapping, MutableMapping, Type
from ..utils.types.services import ServiceT
from .models import Event
from .streams import JoinableT
from .topics import TopicT
from .windows import WindowT

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa


class TableT(MutableMapping, JoinableT, ServiceT):
    StateStore: ClassVar[Type] = None

    app: AppT
    table_name: str
    changelog_topic: TopicT
    default: Any  # noqa: E704
    key_type: Type
    value_type: Type

    @abc.abstractmethod
    def using_window(self, window: WindowT) -> 'WindowWrapperT':
        ...

    @abc.abstractmethod
    def hopping(self, size: float, step: float,
                expires: float = None) -> 'WindowWrapperT':
        ...

    @abc.abstractmethod
    def tumbling(self, size: float, expires: float = None) -> 'WindowWrapperT':
        ...


class WindowSetT(MutableMapping):
    key: Any
    table: TableT
    window: WindowT
    event: Event = None

    @abc.abstractmethod
    def __init__(self,
                 key: Any,
                 table: TableT,
                 window: WindowT,
                 event: Event = None) -> None:
        ...

    @abc.abstractmethod
    def apply(self,
              op: Callable[[Any, Any], Any],
              value: Any,
              event: Event = None) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def timestamp(self, event: Event = None) -> float:
        ...

    @abc.abstractmethod
    def current(self, event: Event = None) -> Any:
        ...

    @abc.abstractmethod
    def delta(self, d: timedelta, event: Event = None) -> Any:
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
