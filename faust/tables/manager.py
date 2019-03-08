"""Tables (changelog stream)."""
import asyncio
from typing import Any, MutableMapping, Optional, Set, Tuple

from mode import Service
from mode.utils.queues import ThrowableQueue

from faust.types import AppT, ChannelT, StoreT, TP
from faust.types.tables import CollectionT, TableManagerT
from faust.utils.tracing import traced_from_parent_span

from .recovery import Recovery

__all__ = [
    'TableManager',
]


class TableManager(Service, TableManagerT):
    """Manage tables used by Faust worker."""

    _channels: MutableMapping[CollectionT, ChannelT]
    _changelogs: MutableMapping[str, CollectionT]
    _recovery_started: asyncio.Event
    _changelog_queue: ThrowableQueue
    _pending_persisted_offsets: MutableMapping[TP, Tuple[StoreT, int]]

    _recovery: Optional[Recovery] = None

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.data: MutableMapping = {}
        self._changelog_queue = None
        self._channels = {}
        self._changelogs = {}
        self._recovery_started = asyncio.Event(loop=self.loop)

        self.actives_ready = False
        self.standbys_ready = False
        self._pending_persisted_offsets = {}

    def persist_offset_on_commit(self,
                                 store: StoreT,
                                 tp: TP,
                                 offset: int) -> None:
        """Mark the persisted offset for a TP to be saved on commit.

        This is used for "exactly_once" processing guarantee.
        Instead of writing the persisted offset to RocksDB when the message
        is sent, we write it to disk when the offset is committed.
        """
        existing_entry = self._pending_persisted_offsets.get(tp)
        if existing_entry is not None:
            _, existing_offset = existing_entry
            if offset < existing_offset:
                return
        self._pending_persisted_offsets[tp] = (store, offset)

    def on_commit(self, offsets: MutableMapping[TP, int]) -> None:
        # flush any pending persisted offsets added by
        # persist_offset_on_commit
        for tp in offsets:
            self.on_commit_tp(tp)

    def on_commit_tp(self, tp: TP) -> None:
        entry = self._pending_persisted_offsets.get(tp)
        if entry is not None:
            store, offset = entry
            store.set_persisted_offset(tp, offset)

    def on_rebalance_start(self) -> None:
        self.actives_ready = False
        self.standbys_ready = False

    def on_actives_ready(self) -> None:
        self.actives_ready = True

    def on_standbys_ready(self) -> None:
        self.standbys_ready = True

    def __hash__(self) -> int:
        return object.__hash__(self)

    @property
    def changelog_topics(self) -> Set[str]:
        return set(self._changelogs.keys())

    @property
    def changelog_queue(self) -> ThrowableQueue:
        if self._changelog_queue is None:
            self._changelog_queue = self.app.FlowControlQueue(
                maxsize=self.app.conf.stream_buffer_maxsize,
                loop=self.loop,
                clear_on_resume=True,
            )
        return self._changelog_queue

    @property
    def recovery(self) -> Recovery:
        if self._recovery is None:
            self._recovery = Recovery(
                self.app, self, beacon=self.beacon, loop=self.loop)
        return self._recovery

    def add(self, table: CollectionT) -> CollectionT:
        if self._recovery_started.is_set():
            raise RuntimeError('Too late to add tables at this point')
        assert table.name is not None
        if table.name in self:
            raise ValueError(f'Table with name {table.name!r} already exists')
        self[table.name] = table
        self._changelogs[table.changelog_topic.get_topic_name()] = table
        return table

    async def on_start(self) -> None:
        await self.sleep(1.0)
        if not self.should_stop:
            await self._update_channels()
            await self.recovery.start()

    async def _update_channels(self) -> None:
        for table in self.values():
            if table not in self._channels:
                chan = table.changelog_topic.clone_using_queue(
                    self.changelog_queue)
                self.app.topics.add(chan)
                self._channels[table] = chan
            await table.maybe_start()
        self.app.consumer.pause_partitions({
            tp for tp in self.app.consumer.assignment()
            if tp.topic in self._changelogs
        })

    async def on_stop(self) -> None:
        await self.app._fetcher.stop()
        if self._recovery:
            await self._recovery.stop()
        for table in self.values():
            await table.stop()

    def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        T = traced_from_parent_span()
        T(self.recovery.on_partitions_revoked)(revoked)

    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        self._recovery_started.set()  # cannot add more tables.
        T = traced_from_parent_span()
        for table in self.values():
            await T(table.on_rebalance)(assigned, revoked, newly_assigned)

        await T(self._update_channels)()
        await T(self.recovery.on_rebalance)(assigned, revoked, newly_assigned)
