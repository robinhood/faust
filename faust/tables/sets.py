from typing import (
    Any,
    Iterable,
    List,
    Set,
    cast,
)

from mode.utils.collections import ManagedUserSet

from faust.types.tables import KT, VT
from faust.types.stores import StoreT

from .objects import ChangeloggedObject, ChangeloggedObjectManager
from .table import Table

__all__ = ['SetTable']

OPERATION_ADD: int = 0x1
OPERATION_DISCARD: int = 0x2
OPERATION_UPDATE: int = 0xF


class ChangeloggedSet(ChangeloggedObject, ManagedUserSet[VT]):

    key: Any
    data: Set

    def __post_init__(self) -> None:
        self.data = set()

    def on_add(self, value: VT) -> None:
        self.manager.send_changelog_event(self.key, OPERATION_ADD, value)

    def on_discard(self, value: VT) -> None:
        self.manager.send_changelog_event(self.key, OPERATION_DISCARD, value)

    def on_change(self, added: Set[VT], removed: Set[VT]) -> None:
        self.manager.send_changelog_event(
            self.key, OPERATION_UPDATE, [added, removed])

    def sync_from_storage(self, value: Any) -> None:
        self.data = cast(Set, value)

    def as_stored_value(self) -> Any:
        return self.data

    def apply_changelog_event(self, operation: int, value: Any) -> None:
        if operation == OPERATION_ADD:
            self.data.add(value)
        elif operation == OPERATION_DISCARD:
            self.data.discard(value)
        elif operation == OPERATION_UPDATE:
            tup = cast(Iterable[List], value)
            added: List
            removed: List
            added, removed = tup
            self.data |= set(added)
            self.data -= set(removed)
        else:
            raise NotImplementedError(
                f'Unknown operation {operation}: key={self.key!r}')


class ChangeloggedSetManager(ChangeloggedObjectManager):
    ValueType = ChangeloggedSet


class SetTable(Table[KT, VT]):

    def _new_store(self) -> StoreT:
        return ChangeloggedSetManager(self)

    def __getitem__(self, key: KT) -> ChangeloggedSet[VT]:
        # FastUserDict looks up using `key in self.data`
        # but we are a defaultdict.
        return self.data[key]
