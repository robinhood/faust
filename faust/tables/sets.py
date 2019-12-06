"""Storing sets in tables."""
from enum import Enum
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Type,
    cast,
)

from mode import Service
from mode.utils.collections import ManagedUserSet
from mode.utils.objects import cached_property
from yarl import URL

from faust.models import Record, maybe_model
from faust.types import AgentT, AppT, EventT, StreamT, TopicT
from faust.types.tables import GlobalTableT, KT, VT
from faust.types.stores import StoreT

from . import wrappers
from .objects import ChangeloggedObject, ChangeloggedObjectManager
from .table import Table

__all__ = ['SetTable', 'SetGlobalTable']

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

    def __iter__(self) -> Iterator[VT]:
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

    url = cast(URL, None)  # fix deep going test case error.

    ValueType = ChangeloggedSet


class SetAction(Enum):
    ADD = 'ADD'
    DISCARD = 'DISCARD'
    CLEAR = 'CLEAR'
    INTERSECTION = 'INTERSECTION'
    SYMDIFF = 'SYMDIFF'


class SetManagerOperation(Record,
                          namespace='@SetManagerOperation'):
    action: SetAction
    members: List[Any]


class SetTableManager(Service, Generic[KT, VT]):
    """Manager used to perform operations on :class:`SetTable`.

    Used when set table is configured with ``SetTable('name',
    start_manager=True)``.

    The workers will start an additional agent used to process
    incoming set operations, and you can communicate with this
    agent to modify your sets.

    Examples:
        >>> set_table = SetTable('sets', start_manager=True)

        >>> await set_table.manager.add('active_orders', Order)
        >>> await set_table.manager.discard('active_orders', Order)

    The manager methods can be used from HTTP views and other agents
    to safely route set operations to the correct worker.
    """

    app: AppT
    set_table: 'SetTable[KT, VT]'
    enabled: bool

    agent: Optional[AgentT]
    actions: Dict[SetAction, Callable[[KT, List[VT]], None]]

    def __init__(self, set_table: 'SetTable[KT, VT]',
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.set_table = set_table
        self.app = self.set_table.app
        self.enabled = self.set_table.start_manager
        self.actions = {
            SetAction.ADD: self._update,
            SetAction.DISCARD: self._difference_update,
            SetAction.CLEAR: self._clear,
            SetAction.INTERSECTION: self._intersection_update,
            SetAction.SYMDIFF: self._symmetric_difference_update,
        }
        if self.enabled:
            self._enable()

    async def add(self, key: KT, member: VT) -> None:
        """Add member to set table using key."""
        await self._send_operation(SetAction.ADD, key, [member])

    async def discard(self, key: KT, member: VT) -> None:
        """Discard member from set table using key."""
        await self._send_operation(SetAction.DISCARD, key, [member])

    async def clear(self, key: KT) -> None:
        """Clear all members from set table using key."""
        await self._send_operation(SetAction.CLEAR, key, [])

    async def difference_update(self, key: KT, members: Iterable[VT]) -> None:
        """Remove members from set with key."""
        await self._send_operation(SetAction.DISCARD, key, members)

    async def intersection_update(self,
                                  key: KT,
                                  members: Iterable[VT]) -> None:
        """Update the set with key to be the intersection of another set.

        This will keep all members that are in both sets.
        """
        await self._send_operation(SetAction.INTERSECTION, key, members)

    async def symmetric_difference_update(self,
                                          key: KT,
                                          members: Iterable[VT]) -> None:
        """Update set by key to be the symmetric difference of another set.

        Members common to both sets will be removed.
        """
        await self._send_operation(SetAction.SYMDIFF, key, members)

    def _update(self, key: KT, members: List[VT]) -> None:
        self.set_table[key].update(members)

    def _difference_update(self, key: KT, members: List[VT]) -> None:
        self.set_table[key].difference_update(members)

    def _clear(self, key: KT, members: List[VT]) -> None:
        self.set_table[key].clear()

    def _intersection_update(self, key: KT, members: List[VT]) -> None:
        self.set_table[key].intersection_update(members)

    def _symmetric_difference_update(self, key: KT, members: List[VT]) -> None:
        self.set_table[key].symmetric_difference_update(members)

    async def _send_operation(self,
                              action: SetAction,
                              key: KT,
                              members: Iterable[VT]) -> None:
        if not self.enabled:
            raise RuntimeError(
                f'Set table {self.set_table} is start_manager=False')
        if iter(members) is members:
            members = list(members)
        await self.topic.send(
            key=key,
            value=SetManagerOperation(action=action, members=members),
        )

    def _enable(self) -> None:
        self.agent = self.app.agent(
            channel=self.topic,
            name='faust.SetTable.manager',
        )(self._modify_set)

    async def _modify_set(self, stream: StreamT[SetManagerOperation]) -> None:
        actions = self.actions
        _maybe_model = maybe_model
        async for set_key, set_operation in stream.items():
            try:
                action = SetAction(set_operation.action)
            except ValueError:
                self.log.exception(
                    'Unknown set operation: %r', set_operation.action)
            else:
                members = [_maybe_model(m) for m in set_operation.members]
                handler = actions[action]
                handler(set_key, members)

    @cached_property
    def topic(self) -> TopicT:
        """Return topic used by set table manager."""
        return self.app.topic(self.set_table.manager_topic_name,
                              key_type=str,
                              value_type=SetManagerOperation)


class SetTable(Table[KT, ChangeloggedSet[VT]]):
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
        """Call when set table starts."""
        if self.start_manager:
            await self.add_runtime_dependency(self.manager)
        await super().on_start()

    def _new_store(self) -> StoreT:
        return ChangeloggedSetManager(self)

    def __getitem__(self, key: KT) -> ChangeloggedSet[VT]:
        # FastUserDict looks up using `key in self.data`
        # but we are a defaultdict.
        return self.data[key]


class SetGlobalTable(SetTable, GlobalTableT):
    pass
