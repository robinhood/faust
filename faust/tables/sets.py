from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Set,
    cast,
)

from mode import Service
from mode.utils.collections import FastUserDict, ManagedUserSet

from faust.streams import current_event
from faust.types import EventT, TP
from faust.types.tables import CollectionT, KT, VT
from faust.types.stores import StoreT

from .table import Table

__all__ = ['SetTable']

OPERATION_ADD: int = 0x1
OPERATION_DISCARD: int = 0x2
OPERATION_UPDATE: int = 0xF


class ChangeloggedSet(ManagedUserSet[VT]):

    table: Table
    key: Any
    data: Set

    def __init__(self,
                 table: Table,
                 manager: 'ChangeloggedSetManager',
                 key: Any):
        self.table = table
        self.manager = manager
        self.key = key
        self.data = set()

    def on_add(self, value: VT) -> None:
        event = current_event()
        self.manager.mark_changed(self.key)
        self.table._send_changelog(
            event, (OPERATION_ADD, self.key), value)

    def on_discard(self, value: VT) -> None:
        event = current_event()
        self.manager.mark_changed(self.key)
        self.table._send_changelog(
            event, (OPERATION_DISCARD, self.key), value)

    def on_change(self, added: Set[VT], removed: Set[VT]) -> None:
        event = current_event()
        self.manager.mark_changed(self.key)
        self.table._send_changelog(
            event, (OPERATION_UPDATE, self.key), [added, removed])


class ChangeloggedSetManager(FastUserDict[KT, VT], Service):

    table: Table[KT, VT]
    data: MutableMapping[KT, ChangeloggedSet[VT]]

    _storage: Optional[StoreT] = None
    _dirty: Set[KT]

    def __init__(self, table: Table[KT, VT], **kwargs: Any) -> None:
        self.table = table
        self.data = {}
        self._dirty = set()
        Service.__init__(self, **kwargs)

    def mark_changed(self, key: KT) -> None:
        self._dirty.add(key)

    def __getitem__(self, key: KT) -> ChangeloggedSet[VT]:
        if key in self.data:
            return self.data[key]
        s = self.data[key] = ChangeloggedSet(self.table, self, key)
        return s

    def __setitem__(self, key: KT, value: VT) -> None:
        raise NotImplementedError(f'{self._table_type_name}: cannot set key')

    def __delitem__(self, key: KT) -> None:
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

            operation, key_ = event.key
            key = cast(KT, key_)
            value: Any = event.value
            if operation == OPERATION_ADD:
                element = cast(VT, value)
                self[key].data.add(element)
            elif operation == OPERATION_DISCARD:
                element = cast(VT, value)
                self[key].data.discard(element)
            elif operation == OPERATION_UPDATE:
                tup = cast(Iterable[List[VT]], value)
                added: List[VT]
                removed: List[VT]
                added, removed = tup
                self[key].data |= set(added)
                self[key].data -= set(removed)
            else:
                raise NotImplementedError(
                    f'Unknown operation {operation}: key={event.key!r}')

        for tp, offset in tp_offsets.items():
            self.set_persisted_offset(tp, offset)


class SetTable(Table[KT, VT]):

    def _new_store(self) -> StoreT:
        return ChangeloggedSetManager(self)

    def __getitem__(self, key: KT) -> ChangeloggedSet[VT]:
        # FastUserDict looks up using `key in self.data`
        # but we are a defaultdict.
        return self.data[key]
