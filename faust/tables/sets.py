"""Storing sets in tables."""
from enum import Enum
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    Type,
    cast,
)

from mode import Service
from mode.utils.collections import ManagedUserSet
from mode.utils.objects import cached_property

from faust.models import Record, maybe_model
from faust.types import AgentT, AppT, EventT, StreamT, TopicT
from faust.types.tables import KT, VT
from faust.types.stores import StoreT

from . import wrappers
from .objects import ChangeloggedObject, ChangeloggedObjectManager
from .table import Table

__all__ = ['SetTable']

OPERATION_ADD: int = 0x1
OPERATION_DISCARD: int = 0x2
OPERATION_UPDATE: int = 0xF


class SetWindowSet(wrappers.WindowSet):
    """A windowed set."""

    def add(self, element: Any, *, event: EventT = None) -> None:
        self._apply_set_operation('add', element, event)

    def discard(self, element: Any, *, event: EventT = None) -> None:
        self._apply_set_operation('discard', element, event)

    def _apply_set_operation(self,
                             op: str,
                             element: Any,
                             event: EventT = None) -> None:
        table = cast(Table, self.table)
        timestamp = self.wrapper.get_timestamp(event or self.event)
        key = self.key
        get_ = table._get_key
        self.wrapper.on_set_key(key, element)
        # apply set operation to every window within range of timestamp.
        for window_range in table._window_ranges(timestamp):
            set_wrapper = get_((key, window_range))
            getattr(set_wrapper, op)(element)


class SetWindowWrapper(wrappers.WindowWrapper):
    """Window wrapper for sets."""

    ValueType = SetWindowSet


class ChangeloggedSet(ChangeloggedObject, ManagedUserSet[VT]):
    """A single set in a dictionary of sets."""

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

    def __iter__(self) -> Iterable[VT]:
        return iter(self.data)

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
    """Store that maintains a dictionary of sets."""

    ValueType = ChangeloggedSet


class SetAction(Enum):
    ADD = 'ADD'
    DISCARD = 'DISCARD'


class SetManagerOperation(Record, Generic[VT],
                          namespace='@SetManagerOperation'):
    action: SetAction
    member: VT


class SetTableManager(Service, Generic[KT, VT]):
    app: AppT
    set_table: 'SetTable[KT, VT]'
    enabled: bool

    agent: Optional[AgentT]
    actions: Dict[SetAction, Callable[[KT, VT], None]]

    def __init__(self, set_table: 'SetTable[KT, VT]',
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.set_table = set_table
        self.app = self.set_table.app
        self.enabled = self.set_table.start_manager
        self.actions = {
            SetAction.ADD: self._add,
            SetAction.DISCARD: self._discard,
        }
        if self.enabled:
            self._enable()

    async def add(self, key: KT, member: VT) -> None:
        await self._send_operation(SetAction.ADD, key, member)

    async def discard(self, key: KT, member: VT) -> None:
        await self._send_operation(SetAction.DISCARD, key, member)

    def _add(self, key: KT, member: VT) -> None:
        self.set_table[key].add(member)

    def _discard(self, key: KT, member: VT) -> None:
        self.set_table[key].discard(member)

    async def _send_operation(self,
                              action: SetAction,
                              key: KT,
                              member: VT) -> None:
        if not self.enabled:
            raise RuntimeError(
                f'Set table {self.set_table} is start_manager=False')
        await self.topic.send(
            key=key,
            value=SetManagerOperation(action=action, member=member),
        )

    def _enable(self) -> None:
        self.agent = self.app.agent(
            channel=self.topic,
            name='faust.SetTable.manager',
        )(self._modify_set)

    async def _modify_set(self, stream: StreamT[SetManagerOperation]) -> None:
        async for set_key, set_operation in stream.items():
            action = SetAction(set_operation.action)
            member_obj = maybe_model(set_operation.member)
            try:
                handler = self.actions[action]
            except KeyError:
                self.log.exception('Unknown set operation: %r', action)
            else:
                handler(set_key, member_obj)

    @cached_property
    def topic(self) -> TopicT:
        return self.app.topic(self.set_table.manager_topic_name,
                              key_type=str,
                              value_type=SetManagerOperation)


class SetTable(Table[KT, VT]):
    """Table that maintains a dictionary of sets."""
    Manager: ClassVar[Type[SetTableManager]] = SetTableManager
    start_manager: bool
    manager_topic_name: str
    manager_topic_suffix: str = '-setmanager'

    manager: SetTableManager

    WindowWrapper = SetWindowWrapper
    _changelog_compacting = False

    def __init__(self, app: AppT, *,
                 start_manager: bool = False,
                 manager_topic_name: str = None,
                 manager_topic_suffix: str = None,
                 **kwargs: Any) -> None:
        super().__init__(app, **kwargs)
        self.start_manager = start_manager
        if manager_topic_suffix is not None:
            self.manager_topic_suffix = manager_topic_suffix
        if manager_topic_name is None:
            manager_topic_name = self.name + self.manager_topic_suffix
        self.manager_topic_name = manager_topic_name

        self.manager = self.Manager(self, loop=self.loop, beacon=self.beacon)

    async def on_start(self) -> None:
        await self.add_runtime_dependency(self.manager)
        await super().on_start()

    def _new_store(self) -> StoreT:
        return ChangeloggedSetManager(self)

    def __getitem__(self, key: KT) -> ChangeloggedSet[VT]:
        # FastUserDict looks up using `key in self.data`
        # but we are a defaultdict.
        return self.data[key]
