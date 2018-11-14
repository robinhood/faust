"""Table (key/value changelog stream)."""
import sys
from operator import itemgetter
from typing import (
    Any,
    Callable,
    Dict,
    IO,
    Iterable,
    List,
    MutableMapping,
    MutableSet,
    Optional,
    Set,
    cast,
)

from mode import Seconds, Service
from mode.utils import text
from mode.utils.collections import (
    FastUserDict,
    ManagedUserDict,
    ManagedUserSet,
)

from faust import windows
from faust.streams import current_event
from faust.types import EventT, TP
from faust.types.tables import CollectionT, TableT, WindowWrapperT
from faust.types.stores import StoreT
from faust.types.windows import WindowT
from faust.utils import terminal

from .base import Collection
from .wrappers import WindowWrapper

__all__ = ['Table']


class Table(TableT, Collection, ManagedUserDict):
    """Table (non-windowed)."""

    def using_window(self, window: WindowT) -> WindowWrapperT:
        self.window = window
        self._changelog_compacting = True
        self._changelog_deleting = True
        self._changelog_topic = None  # will reset on next property access
        return WindowWrapper(self)

    def hopping(self, size: Seconds, step: Seconds,
                expires: Seconds = None) -> WindowWrapperT:
        return self.using_window(windows.HoppingWindow(size, step, expires))

    def tumbling(self, size: Seconds,
                 expires: Seconds = None) -> WindowWrapperT:
        return self.using_window(windows.TumblingWindow(size, expires))

    def __missing__(self, key: Any) -> Any:
        if self.default is not None:
            return self.default()
        raise KeyError(key)

    def _has_key(self, key: Any) -> bool:
        return key in self

    def _get_key(self, key: Any) -> Any:
        return self[key]

    def _set_key(self, key: Any, value: Any) -> None:
        self[key] = value

    def _del_key(self, key: Any) -> None:
        del self[key]

    def on_key_get(self, key: Any) -> None:
        self._sensor_on_get(self, key)

    def on_key_set(self, key: Any, value: Any) -> None:
        event = current_event()
        self._send_changelog(event, key, value)
        if event is not None:
            partition = event.message.partition
            self._maybe_set_key_ttl(key, partition)
            self._sensor_on_set(self, key, value)
        else:
            raise TypeError(
                'Setting table key from outside of stream iteration')

    def on_key_del(self, key: Any) -> None:
        event = current_event()
        self._send_changelog(event, key, value=None, value_serializer='raw')
        if event is not None:
            partition = event.message.partition
            self._maybe_del_key_ttl(key, partition)
            self._sensor_on_del(self, key)
        else:
            raise TypeError(
                'Deleting table key from outside of stream iteration')

    def as_ansitable(self,
                     *,
                     key: str = 'Key',
                     value: str = 'Value',
                     sort: bool = False,
                     sortkey: Callable[[Any], Any] = itemgetter(0),
                     target: IO = sys.stdout,
                     title: str = '{table.name}') -> str:
        header = [text.title(key), text.title(value)]
        data = cast(Iterable[List[str]], dict(self).items())
        data = list(sorted(data, key=sortkey)) if sort else list(data)
        if sort:
            data = list(sorted(data, key=sortkey))
        return terminal.table(
            [header] + list(data),
            title=text.title(title.format(table=self)),
        ).table


OPERATION_ADD = 0x1
OPERATION_DISCARD = 0x2


class ChangeloggedSet(ManagedUserSet):

    table: Table
    key: Any
    data: MutableSet

    def __init__(self,
                 table: Table,
                 manager: 'ChangeloggedSetManager',
                 key: Any):
        self.table = table
        self.manager = manager
        self.key = key
        self.data = set()

    def on_add(self, value: Any) -> None:
        event = current_event()
        self.manager.mark_changed(self.key)
        self.table._send_changelog(
            event, (OPERATION_ADD, self.key), value)

    def on_discard(self, value: Any) -> None:
        event = current_event()
        self.manager.mark_changed(self.key)
        self.table._send_changelog(
            event, (OPERATION_DISCARD, self.key), value)


class ChangeloggedSetManager(Service, FastUserDict):

    table: Table
    data: MutableMapping

    _storage: Optional[StoreT] = None
    _dirty: Set[Any]

    def __init__(self, table: Table, **kwargs: Any) -> None:
        self.table = table
        self.data = {}
        self._dirty = set()
        Service.__init__(self, **kwargs)

    def mark_changed(self, key: Any) -> None:
        self._dirty.add(key)

    def __getitem__(self, key: Any) -> ChangeloggedSet:
        if key in self.data:
            return self.data[key]
        s = self.data[key] = ChangeloggedSet(self.table, self, key)
        return s

    def __setitem__(self, key: Any, value: Any) -> None:
        raise NotImplementedError(f'{self._table_type_name}: cannot set key')

    def __delitem__(self, key: Any) -> None:
        raise NotImplementedError(f'{self._table_type_name}: cannot del key')

    @property
    def _table_type_name(self) -> str:
        return f'{type(self.table).__name__}'

    async def on_start(self) -> None:
        await self.add_runtime_dependency(self.storage)

    async def on_stop(self) -> None:
        await self.flush_to_storage()

    def persisted_offset(self, tp: TP) -> Optional[int]:
        return self.storage.persisted_offset(tp)

    def set_persisted_offset(self, tp: TP, offset: int) -> None:
        self.storage.set_persisted_offset(tp, offset)

    async def on_rebalance(self,
                           table: CollectionT,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        await self.storage.on_rebalance(
            table, assigned, revoked, newly_assigned)

    async def on_recovery_completed(self,
                                    active_tps: Set[TP],
                                    standby_tps: Set[TP]) -> None:
        await self.sync_from_storage()
        await super().on_recovery_completed(active_tps, standby_tps)

    async def sync_from_storage(self) -> None:
        for key, value in self.storage.items():
            self[key].data = value

    async def flush_to_storage(self) -> None:
        for key in self._dirty:
            self.storage[key] = self.data[key].data
        self._dirty.clear()

    @Service.task(2.0)
    async def _periodic_flush(self) -> None:
        await self.flush_to_storage()

    @property
    def storage(self) -> StoreT:
        if self._storage is None:
            self._storage = self.table._new_store_by_url(
                self.table._store or self.table.app.conf.store)
        return self._storage

    def apply_changelog_batch(self,
                              batch: Iterable[EventT],
                              to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        tp_offsets: Dict[TP, int] = {}
        for event in batch:
            tp, offset = event.message.tp, event.message.offset
            tp_offsets[tp] = (
                offset if tp not in tp_offsets
                else max(offset, tp_offsets[tp])
            )

            if event.key is None:
                raise RuntimeError('Changelog key cannot be None')

            operation, key = event.key
            value = event.value
            if operation == OPERATION_ADD:
                self[key].data.add(value)
            elif operation == OPERATION_DISCARD:
                self[key].data.discard(value)
            else:
                raise NotImplementedError(
                    f'Unknown operation {operation}: key={event.key!r}')

        for tp, offset in tp_offsets.items():
            self.set_persisted_offset(tp, offset)


class SetTable(Table):

    def _new_store(self) -> StoreT:
        return ChangeloggedSetManager(self)

    def __getitem__(self, key: Any) -> Any:
        # FastUserDict looks up using `key in self.data`
        # but we are a defaultdict.
        return self.data[key]
