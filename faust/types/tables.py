import abc
import typing
from datetime import datetime
from typing import (
    Any,
    Awaitable,
    Callable,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    ValuesView,
)

from mode import Seconds, ServiceT
from mode.utils.collections import FastUserDict, ManagedUserDict
from yarl import URL

from .events import EventT
from .stores import StoreT
from .streams import JoinableT
from .topics import TopicT
from .tuples import TP
from .windows import WindowT

if typing.TYPE_CHECKING:
    from .app import AppT as _AppT
    from .models import FieldDescriptorT as _FieldDescriptorT
    from .models import ModelArg as _ModelArg
else:
    class _AppT: ...  # noqa
    class _FieldDescriptorT: ...  # noqa
    class _ModelArg: ...  # noqa

__all__ = [
    'RecoverCallback',
    'RelativeArg',
    'CollectionT',
    'TableT',
    'TableManagerT',
    'WindowSetT',
    'WindowedItemsViewT',
    'WindowedValuesViewT',
    'WindowWrapperT',
    'ChangelogEventCallback',
    'CollectionTps',
]

RelativeHandler = Callable[[Optional[EventT]], Union[float, datetime]]
RecoverCallback = Callable[[], Awaitable[None]]
ChangelogEventCallback = Callable[[EventT], Awaitable[None]]
RelativeArg = Optional[Union[
    _FieldDescriptorT,
    RelativeHandler,
    datetime,
    float,
]]
CollectionTps = MutableMapping['CollectionT', Set[TP]]

KT = TypeVar('KT')
VT = TypeVar('VT')


class CollectionT(ServiceT, JoinableT):
    app: _AppT
    name: str
    default: Any  # noqa: E704
    key_type: Optional[_ModelArg]
    value_type: Optional[_ModelArg]
    partitions: Optional[int]
    window: Optional[WindowT]
    help: str
    recovery_buffer_size: int
    standby_buffer_size: int

    @abc.abstractmethod
    def __init__(self,
                 app: _AppT,
                 *,
                 name: str = None,
                 default: Callable[[], Any] = None,
                 store: Union[str, URL] = None,
                 key_type: _ModelArg = None,
                 value_type: _ModelArg = None,
                 partitions: int = None,
                 window: WindowT = None,
                 changelog_topic: TopicT = None,
                 help: str = None,
                 on_recover: RecoverCallback = None,
                 on_changelog_event: ChangelogEventCallback = None,
                 recovery_buffer_size: int = 1000,
                 standby_buffer_size: int = None,
                 extra_topic_configs: Mapping[str, Any] = None,
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
    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        ...

    @abc.abstractmethod
    async def on_changelog_event(self, event: EventT) -> None:
        ...

    @abc.abstractmethod
    def on_recover(self, fun: RecoverCallback) -> RecoverCallback:
        ...

    @abc.abstractmethod
    async def on_recovery_completed(self,
                                    active_tps: Set[TP],
                                    standby_tps: Set[TP]) -> None:
        ...

    @abc.abstractmethod
    async def call_recover_callbacks(self) -> None:
        ...


class TableT(CollectionT, ManagedUserDict[KT, VT]):

    @abc.abstractmethod
    def using_window(self, window: WindowT, *,
                     key_index: bool = False) -> 'WindowWrapperT':
        ...

    @abc.abstractmethod
    def hopping(self, size: Seconds, step: Seconds,
                expires: Seconds = None,
                key_index: bool = False) -> 'WindowWrapperT':
        ...

    @abc.abstractmethod
    def tumbling(self, size: Seconds,
                 expires: Seconds = None,
                 key_index: bool = False) -> 'WindowWrapperT':
        ...

    @abc.abstractmethod
    def as_ansitable(self, **kwargs: Any) -> str:
        ...


class TableManagerT(ServiceT, FastUserDict[str, CollectionT]):
    app: _AppT

    actives_ready: bool
    standbys_ready: bool

    @abc.abstractmethod
    def __init__(self, app: _AppT, **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def add(self, table: CollectionT) -> CollectionT:
        ...

    @abc.abstractmethod
    def persist_offset_on_commit(self,
                                 store: StoreT,
                                 tp: TP,
                                 offset: int) -> None:
        ...

    @abc.abstractmethod
    def on_commit(self, offsets: MutableMapping[TP, int]) -> None:
        ...

    @abc.abstractmethod
    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        ...

    @property
    @abc.abstractmethod
    def changelog_topics(self) -> Set[str]:
        ...


class WindowSetT(FastUserDict[KT, VT]):
    key: Any
    table: TableT
    event: Optional[EventT]

    @abc.abstractmethod
    def __init__(self,
                 key: KT,
                 table: TableT,
                 wrapper: 'WindowWrapperT',
                 event: EventT = None) -> None:
        ...

    @abc.abstractmethod
    def apply(self,
              op: Callable[[VT, VT], VT],
              value: VT,
              event: EventT = None) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def value(self, event: EventT = None) -> VT:
        ...

    @abc.abstractmethod
    def current(self, event: EventT = None) -> VT:
        ...

    @abc.abstractmethod
    def now(self) -> VT:
        ...

    @abc.abstractmethod
    def delta(self, d: Seconds, event: EventT = None) -> VT:
        ...

    @abc.abstractmethod
    def __iadd__(self, other: VT) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def __isub__(self, other: VT) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def __imul__(self, other: VT) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def __itruediv__(self, other: VT) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def __ifloordiv__(self, other: VT) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def __imod__(self, other: VT) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def __ipow__(self, other: VT) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def __ilshift__(self, other: VT) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def __irshift__(self, other: VT) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def __iand__(self, other: VT) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def __ixor__(self, other: VT) -> 'WindowSetT':
        ...

    @abc.abstractmethod
    def __ior__(self, other: VT) -> 'WindowSetT':
        ...


class WindowedItemsViewT(ItemsView):

    @abc.abstractmethod
    def __init__(self,
                 mapping: 'WindowWrapperT',
                 event: EventT = None) -> None:
        ...

    @abc.abstractmethod
    def __iter__(self) -> Iterator[Tuple[Any, Any]]:
        ...

    @abc.abstractmethod
    def now(self) -> Iterator[Tuple[Any, Any]]:
        ...

    @abc.abstractmethod
    def current(self, event: EventT = None) -> Iterator[Tuple[Any, Any]]:
        ...

    @abc.abstractmethod
    def delta(self,
              d: Seconds,
              event: EventT = None) -> Iterator[Tuple[Any, Any]]:
        ...


class WindowedValuesViewT(ValuesView):

    @abc.abstractmethod
    def __init__(self,
                 mapping: 'WindowWrapperT',
                 event: EventT = None) -> None:
        ...

    @abc.abstractmethod
    def __iter__(self) -> Iterator[Any]:
        ...

    @abc.abstractmethod
    def now(self) -> Iterator[Any]:
        ...

    @abc.abstractmethod
    def current(self, event: EventT = None) -> Iterator[Any]:
        ...

    @abc.abstractmethod
    def delta(self, d: Seconds, event: EventT = None) -> Iterator[Any]:
        ...


class WindowWrapperT(MutableMapping):
    table: TableT

    @abc.abstractmethod
    def __init__(self, table: TableT, *,
                 relative_to: RelativeArg = None) -> None:
        ...

    @property
    @abc.abstractmethod
    def name(self) -> str:
        ...

    @abc.abstractmethod
    def clone(self, relative_to: RelativeArg) -> 'WindowWrapperT':
        ...

    @abc.abstractmethod
    def relative_to_now(self) -> 'WindowWrapperT':
        ...

    @abc.abstractmethod
    def relative_to_field(self, field: _FieldDescriptorT) -> 'WindowWrapperT':
        ...

    @abc.abstractmethod
    def relative_to_stream(self) -> 'WindowWrapperT':
        ...

    @abc.abstractmethod
    def get_timestamp(self, event: EventT = None) -> float:
        ...

    @abc.abstractmethod
    def __getitem__(self, key: Any) -> WindowSetT:
        ...

    @abc.abstractmethod
    def keys(self) -> KeysView:
        ...

    @abc.abstractmethod
    def on_set_key(self, key: Any, value: Any) -> None:
        ...

    @abc.abstractmethod
    def on_del_key(self, key: Any) -> None:
        ...

    @abc.abstractmethod
    def as_ansitable(self, **kwargs: Any) -> str:
        ...

    @property
    def get_relative_timestamp(self) -> Optional[RelativeHandler]:
        ...

    @get_relative_timestamp.setter
    def get_relative_timestamp(self, relative_to: RelativeArg) -> None:
        ...
