import abc

import asyncio
import typing
from typing import (
    Any, Awaitable, Callable, ClassVar, Iterable, List, MutableMapping,
    MutableSet, Optional, Set, Type, Union,
)
from mode import Seconds, ServiceT
from mode.utils.compat import Counter
from yarl import URL
from .channels import EventT
from .stores import StoreT
from .streams import JoinableT
from .topics import TopicT
from .tuples import TP
from .windows import WindowT

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
    'ChangelogReaderT',
    'CollectionTps',
]


RecoverCallback = Callable[[], Awaitable[None]]


class CollectionT(JoinableT, ServiceT):
    StateStore: ClassVar[Type[StoreT]] = None

    app: AppT
    name: str
    default: Any  # noqa: E704
    key_type: ModelArg
    value_type: ModelArg
    partitions: int
    window: WindowT = None
    help: str

    @abc.abstractmethod
    def __init__(self, app: AppT,
                 *,
                 name: str = None,
                 default: Callable[[], Any] = None,
                 store: Union[str, URL] = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 partitions: int = None,
                 window: WindowT = None,
                 changelog_topic: TopicT = None,
                 help: str = None,
                 on_recover: RecoverCallback = None,
                 **kwargs: Any) -> None:
        ...

    @property
    @abc.abstractmethod
    def changelog_topic(self) -> TopicT:
        ...

    @changelog_topic.setter
    def changelog_topic(self, topic: TopicT) -> None:
        ...

    @abc.abstractmethod
    def apply_changelog_batch(self, batch: Iterable[EventT]) -> None:
        ...

    @abc.abstractmethod
    def persisted_offset(self, tp: TP) -> Optional[int]:
        ...

    @abc.abstractmethod
    async def need_active_standby_for(self, tp: TP) -> bool:
        ...

    @abc.abstractmethod
    def reset_state(self) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_assigned(self, assigned: Iterable[TP]) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_revoked(self, revoked: Iterable[TP]) -> None:
        ...

    @abc.abstractmethod
    def on_recover(self, fun: RecoverCallback) -> RecoverCallback:
        ...

    @abc.abstractmethod
    async def call_recover_callbacks(self) -> None:
        ...

CollectionTps = MutableMapping[CollectionT, List[TP]]


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

    @abc.abstractmethod
    def as_ansitable(self,
                     *,
                     key: str = None,
                     value: str = None,
                     sort: bool = False,
                     sortkey: Callable[[Any], Any] = None,
                     title: str = None) -> str:
        ...


class SetT(CollectionT, MutableSet):
    ...


class TableManagerT(ServiceT, MutableMapping[str, CollectionT]):
    app: AppT
    recovery_completed: asyncio.Event

    @abc.abstractmethod
    def __init__(self, app: AppT, **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def add(self, table: CollectionT) -> CollectionT:
        ...

    @abc.abstractmethod
    async def on_partitions_assigned(self, assigned: Iterable[TP]) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_revoked(self, revoked: Iterable[TP]) -> None:
        ...

    @property
    @abc.abstractmethod
    def changelog_topics(self) -> Set[str]:
        ...


class ChangelogReaderT(ServiceT):
    table: CollectionT
    app: AppT

    tps: Iterable[TP]
    offsets: Counter[TP]


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
