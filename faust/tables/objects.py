"""Storing objects in tables.

This is also used to store data structures such as sets/lists.

"""
import abc

from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    MutableMapping,
    Optional,
    Set,
    Type,
)

from mode import Service

from faust.stores.base import Store
from faust.streams import current_event
from faust.types import EventT, TP
from faust.types.stores import StoreT
from faust.types.tables import CollectionT

from .table import Table


class ChangeloggedObject:
    """A changelogged object in a :class:`ChangeloggedObjectManager` store."""

    manager: 'ChangeloggedObjectManager'

    def __init__(self, manager: 'ChangeloggedObjectManager', key: Any) -> None:
        self.manager = manager
        self.key = key
        self.__post_init__()

    def __post_init__(self) -> None:  # pragma: no cover
        ...

    @abc.abstractmethod
    def sync_from_storage(self, value: Any) -> None:
        """Sync value from storage."""
        ...

    @abc.abstractmethod
    def as_stored_value(self) -> Any:
        """Return value as represented in storage."""
        ...

    @abc.abstractmethod
    def apply_changelog_event(self, operation: int, value: Any) -> None:
        """Apply event in changelog topic to local table state."""
        ...


class ChangeloggedObjectManager(Store):
    """Store of changelogged objects."""

    ValueType: ClassVar[Type[ChangeloggedObject]]

    table: Table
    data: MutableMapping

    _storage: Optional[StoreT] = None
    _dirty: Set

    def __init__(self, table: Table, **kwargs: Any) -> None:
        self.table = table
        self.table_name = self.table.name
        self.data = {}
        self._dirty = set()
        Service.__init__(self, **kwargs)

    def send_changelog_event(self,
                             key: Any,
                             operation: int,
                             value: Any) -> None:
        """Send changelog event to the tables changelog topic."""
        event = current_event()
        self._dirty.add(key)
        self.table._send_changelog(event, (operation, key), value)

    def __getitem__(self, key: Any) -> ChangeloggedObject:
        if key in self.data:
            return self.data[key]
        s = self.data[key] = self.ValueType(self, key)
        return s

    def __setitem__(self, key: Any, value: Any) -> None:
        raise NotImplementedError(f'{self._table_type_name}: cannot set key')

    def __delitem__(self, key: Any) -> None:
        raise NotImplementedError(f'{self._table_type_name}: cannot del key')

    @property
    def _table_type_name(self) -> str:
        return f'{type(self.table).__name__}'

    async def on_start(self) -> None:
        """Call when the changelogged object manager starts."""
        await self.add_runtime_dependency(self.storage)

    async def on_stop(self) -> None:
        """Call when the changelogged object manager stops."""
        self.flush_to_storage()

    def persisted_offset(self, tp: TP) -> Optional[int]:
        """Get the last persisted offset for changelog topic partition."""
        return self.storage.persisted_offset(tp)

    def set_persisted_offset(self, tp: TP, offset: int) -> None:
        """Set the last persisted offset for changelog topic partition."""
        self.storage.set_persisted_offset(tp, offset)

    async def on_rebalance(self,
                           table: CollectionT,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        """Call when cluster is rebalancing."""
        await self.storage.on_rebalance(
            table, assigned, revoked, newly_assigned)

    async def on_recovery_completed(self,
                                    active_tps: Set[TP],
                                    standby_tps: Set[TP]) -> None:
        """Call when table recovery is completed after rebalancing."""
        self.sync_from_storage()

    def sync_from_storage(self) -> None:
        """Sync set contents from storage."""
        for key, value in self.storage.items():
            self[key].sync_from_storage(value)

    def flush_to_storage(self) -> None:
        """Flush set contents to storage."""
        for key in self._dirty:
            self.storage[key] = self.data[key].as_stored_value()
        self._dirty.clear()

    @Service.task
    async def _periodic_flush(self) -> None:  # pragma: no cover
        async for sleep_time in self.itertimer(2.0, name='SetManager.flush'):
            await self.sleep(sleep_time)
            self.flush_to_storage()

    def reset_state(self) -> None:
        """Reset table local state."""
        # delegate to underlying RocksDB store.
        self.storage.reset_state()

    @property
    def storage(self) -> StoreT:
        """Return underlying storage used by this set table."""
        if self._storage is None:
            self._storage = self.table._new_store_by_url(
                self.table._store or self.table.app.conf.store)
        return self._storage

    def apply_changelog_batch(self,
                              batch: Iterable[EventT],
                              to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        """Apply batch of changelog events to local state."""
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
            key = to_key(key)
            value: Any = to_value(event.value)
            self[key].apply_changelog_event(operation, value)

        for tp, offset in tp_offsets.items():
            self.set_persisted_offset(tp, offset)
