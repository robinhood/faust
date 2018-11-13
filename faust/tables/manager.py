"""Tables (changelog stream)."""
import asyncio
from typing import Any, MutableMapping, Optional, Set

from mode import Service
from mode.utils.collections import FastUserDict
from mode.utils.queues import ThrowableQueue

from faust.types import AppT, ChannelT, TP
from faust.types.tables import CollectionT, TableManagerT

from .recovery import Recovery

__all__ = [
    'TableManager',
]


class TableManager(Service, TableManagerT, FastUserDict):
    """Manage tables used by Faust worker."""

    _channels: MutableMapping[CollectionT, ChannelT]
    _changelogs: MutableMapping[str, CollectionT]
    _recovery_started: asyncio.Event
    _changelog_queue: ThrowableQueue

    _recovery: Optional[Recovery] = None

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.data: MutableMapping = {}
        self._changelog_queue = None
        self._channels = {}
        self._changelogs = {}
        self._recovery_started = asyncio.Event(loop=self.loop)

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

    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        await self.recovery.on_partitions_revoked(revoked)

    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        self._recovery_started.set()  # cannot add more tables.
        for table in self.values():
            await table.on_rebalance(assigned, revoked, newly_assigned)

        await self._update_channels()
        await self.recovery.on_rebalance(assigned, revoked, newly_assigned)
