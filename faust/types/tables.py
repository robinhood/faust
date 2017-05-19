import abc
import typing
from typing import Any, Callable, ClassVar, MutableMapping, Type
from ..utils.times import Seconds
from ..utils.types.services import ServiceT
from .streams import JoinableT
from .topics import EventT, TopicT
from .stores import StoreT
from .windows import WindowT

if typing.TYPE_CHECKING:
    from .app import AppT
    from .models import ModelT
else:
    class AppT: ...    # noqa
    class ModelT: ...  # noqa


class TableT(MutableMapping, JoinableT, ServiceT):
    StateStore: ClassVar[Type[StoreT]] = None

    app: AppT
    table_name: str
    default: Any  # noqa: E704
    key_type: Type[ModelT]
    value_type: Type[ModelT]
    partitions: int
    window: WindowT = None

    @abc.abstractmethod
    def __init__(self, app: AppT,
                 *,
                 table_name: str = None,
                 default: Callable[[], Any] = None,
                 store: str = None,
                 key_type: Type[ModelT] = None,
                 value_type: Type[ModelT] = None,
                 partitions: int = None,
                 window: WindowT = None,
                 changelog_topic: TopicT = None,
                 **kwargs: Any) -> None:
        ...

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

    @property
    @abc.abstractmethod
    def changelog_topic(self) -> TopicT:
        ...

    @changelog_topic.setter
    def changelog_topic(self, topic: TopicT) -> None:
        ...


class TableManagerT(ServiceT):
    app: AppT
    tables: MutableMapping[str, TableT]

    @abc.abstractmethod
    def get_table_changelog(self, table_name: str) -> 'TableT':
        ...

    @abc.abstractmethod
    def partition_assign_table_handler(self) -> None:
        ...

    @abc.abstractmethod
    def partition_remove_table_handler(self) -> None:
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
