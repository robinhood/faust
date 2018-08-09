"""Tables (changelog stream)."""
import asyncio
from collections import defaultdict
from typing import Any, List, MutableMapping, Optional, Set, cast

from mode import Service
from mode.utils.aiter import aiter
from mode.utils.collections import FastUserDict
from mode.utils.compat import Counter

from faust.types import AppT, ChannelT, TP
from faust.types.tables import (
    ChangelogReaderT,
    CollectionT,
    CollectionTps,
    TableManagerT,
)
from faust.utils import terminal

from .changelogs import ChangelogReader, StandbyReader
from .table import Table

__all__ = [
    'TableManager',
]

TABLEMAN_UPDATE = 'UPDATE'
TABLEMAN_START_STANDBYS = 'START_STANDBYS'
TABLEMAN_STOP_STANDBYS = 'STOP_STANDBYS'
TABLEMAN_RECOVER = 'RECOVER'
TABLEMAN_PARTITIONS_REVOKED = 'PARTITIONS REVOKED'
TABLEMAN_PARTITIONS_ASSIGNED = 'PARTITIONS_ASSIGNED'


class TableManager(Service, TableManagerT, FastUserDict):
    """Manage tables used by Faust worker."""

    _channels: MutableMapping[CollectionT, ChannelT]
    _changelogs: MutableMapping[str, CollectionT]
    _table_offsets: Counter[TP]
    _standbys: MutableMapping[CollectionT, ChangelogReaderT]
    _revivers: Optional[List[ChangelogReaderT]] = None
    _ongoing_recovery: Optional[asyncio.Future] = None
    _recovery_started: asyncio.Event
    recovery_completed: asyncio.Event

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.data: MutableMapping = {}
        self._channels = {}
        self._changelogs = {}
        self._table_offsets = Counter()
        self._standbys = {}
        self._recovery_started = asyncio.Event(loop=self.loop)
        self.recovery_completed = asyncio.Event(loop=self.loop)

    def __hash__(self) -> int:
        return object.__hash__(self)

    @property
    def changelog_topics(self) -> Set[str]:
        return set(self._changelogs.keys())

    def add(self, table: CollectionT) -> CollectionT:
        if self._recovery_started.is_set():
            raise RuntimeError('Too late to add tables at this point')
        assert table.name is not None
        if table.name in self:
            raise ValueError(f'Table with name {table.name!r} already exists')
        self[table.name] = table
        return table

    async def on_start(self) -> None:
        await self.sleep(1.0)
        await self._update_channels()

    @Service.transitions_to(TABLEMAN_UPDATE)
    async def _update_channels(self) -> None:
        for table in self.values():
            if table not in self._channels:
                it = aiter(table.changelog_topic)
                self._channels[table] = cast(ChannelT, it)
        self._changelogs.update({
            table.changelog_topic.get_topic_name(): table
            for table in self.values()
        })
        await self.app.consumer.pause_partitions({
            tp for tp in self.app.consumer.assignment()
            if tp.topic in self._changelogs
        })

    async def on_stop(self) -> None:
        await self.app._fetcher.stop()
        await self._maybe_abort_ongoing_recovery()
        await self._stop_standbys()
        for table in self.values():
            await table.stop()

    async def _maybe_abort_ongoing_recovery(self) -> None:
        if self._ongoing_recovery is not None:
            self.log.info('Aborting ongoing recovery to start over')
            if not self._ongoing_recovery.done():
                # TableManager.stop() will now block until all revivers are
                # stopped. This is expected. Ideally the revivers should stop
                # almost immediately upon receiving a stop()
                if self._revivers:
                    self.log.info('Waiting for %s revivers to complete',
                                  len(self._revivers))
                    for reviver in self._revivers:
                        reviver.set_shutdown()
                    try:
                        await asyncio.wait(
                            [reviver.stop() for reviver in self._revivers])
                    except asyncio.CancelledError:
                        pass
                    self._revivers = None
                ongoing = self._ongoing_recovery
                if ongoing is not None and not ongoing.done():
                    self.log.info('Waiting for ongoing recovery to stop')
                    ongoing.cancel()
                    try:
                        await ongoing
                    except asyncio.CancelledError:
                        pass

                self.log.info('Ongoing recovery halted')
            self._ongoing_recovery = None

    @Service.transitions_to(TABLEMAN_STOP_STANDBYS)
    async def _stop_standbys(self) -> None:
        for standby in self._standbys.values():
            self.log.info('Stopping standby for tps: %s', standby.tps)
            standby.set_shutdown()
            try:
                await standby.stop()
            except asyncio.CancelledError:
                pass
            self._sync_offsets(standby)
        self._standbys = {}

    def _sync_offsets(self, reader: ChangelogReaderT) -> None:
        table = terminal.logtable(
            [(k.topic, k.partition, v) for k, v in reader.offsets.items()],
            title='Sync Offset',
            headers=['topic', 'partition', 'offset'],
        )
        self.log.info('Syncing offsets:\n%s', table)
        for tp, offset in reader.offsets.items():
            if offset >= 0:
                table_offset = self._table_offsets.get(tp, -1)
                self._table_offsets[tp] = max(table_offset, offset)
        table = terminal.logtable(
            [(k.topic, k.partition, v)
             for k, v in self._table_offsets.items()],
            title='Table Offsets',
            headers=['topic', 'partition', 'offset'],
        )
        self.log.info('After syncing:\n%s', table)

    @Service.transitions_to(TABLEMAN_PARTITIONS_REVOKED)
    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        on_timeout = self.app._on_revoked_timeout
        on_timeout.info('+TABLES: maybe_abort_ongoing_recovery')
        await self._maybe_abort_ongoing_recovery()
        on_timeout.info('+TABLES: STOP STANDBYS')
        await self._stop_standbys()
        on_timeout.info(
            f'+TABLES: call table.on_..._revoked {len(self.values())}')
        for table in self.values():
            on_timeout.info(f'+TABLE.on_partitions_revoked(): {table!r}')
            await table.on_partitions_revoked(revoked)
        on_timeout.info(
            f'-TABLES: call table.on_..._revoked {len(self.values())}')

    @Service.transitions_to(TABLEMAN_PARTITIONS_ASSIGNED)
    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        await self._start_recovery(assigned)

    async def _start_recovery(self, assigned: Set[TP]) -> None:
        assert self._ongoing_recovery is None
        assert not self._revivers
        self._ongoing_recovery = self.add_future(self._recover(assigned))
        self.log.info('Triggered recovery in background')

    async def _recover(self, assigned: Set[TP]) -> None:
        standby_tps = self.app.assignor.assigned_standbys()
        # for table in self.values():
        #     standby_tps = await local_tps(table, standby_tps)
        assigned_tps = self.app.assignor.assigned_actives()
        assert set(assigned_tps).issubset(assigned)
        self.log.info('New assignments found')
        # This needs to happen in background and be aborted midway
        await self._on_recovery_started()
        for table in self.values():
            await table.on_partitions_assigned(assigned)
        did_recover = await self._recover_changelogs(assigned_tps)

        if did_recover and not self._stopped.is_set():
            self.log.info('Restore complete!')
            # This needs to happen if all goes well
            callback_coros = [
                table.call_recover_callbacks() for table in self.values()
            ]
            if callback_coros:
                await asyncio.wait(callback_coros)
            await self.app.consumer.perform_seek()
            await self._start_standbys(standby_tps)
            self.log.info('New assignments handled')
            await self._on_recovery_completed()
            await self.app.consumer.resume_partitions({
                tp for tp in assigned
                if not self._is_changelog_tp(tp)
            })
            # finally start the fetcher
            await self.app._fetcher.start()
            self.app.rebalancing = False
            self.log.info('Worker ready')
        else:
            self.log.info('Recovery interrupted')
        self._revivers = None

    async def _on_recovery_started(self) -> None:
        self._recovery_started.set()
        await self._update_channels()

    async def _on_recovery_completed(self) -> None:
        for table in self.values():
            await table.maybe_start()
        self.recovery_completed.set()

    @Service.transitions_to(TABLEMAN_RECOVER)
    async def _recover_changelogs(self, tps: Set[TP]) -> bool:
        self.log.info('Restoring state from changelog topics...')
        table_revivers = self._revivers = [
            self._create_reviver(table, tps) for table in self.values()
        ]
        for reviver in table_revivers:
            await reviver.start()
            self.log.info('Started restoring: %s', reviver.label)
        await self.app._fetcher.start()

        # XXX [asksol] This used to call:
        # asyncio.gather(*[r.wait_done_reading() for r in table_revivers]
        # But on Python 3.7 this hangs forever with 99% CPU.
        # Is this a bug in asyncio?
        # wait_done_reading simply waits for r._stop_event.wait()

        # As a workaround we don't wait for asyncio.Events that are
        # already done.
        pending_revivers = [
            r for r in table_revivers
            if not r._stop_event.is_set()
        ]
        if pending_revivers:
            self.log.info('Waiting for restore to finish...')
            await asyncio.gather(
                *[r._stop_event.wait() for r in pending_revivers],
                loop=self.loop,
            )

        self.log.info('Done reading all changelogs')
        for reviver in table_revivers:
            self._sync_offsets(reviver)
        self.log.info('Done reading from changelog topics')
        await self.app._fetcher.stop()
        self.app._fetcher.service_reset()
        for reviver in table_revivers:
            await reviver.stop()
            self.log.info('Stopped restoring: %s', reviver.label)
        self._revivers = None
        self.log.info('Stopped restoring')
        return all(reviver.recovered() for reviver in table_revivers)

    def _create_reviver(self, table: CollectionT,
                        tps: Set[TP]) -> ChangelogReaderT:
        table = cast(Table, table)
        offsets = self._table_offsets
        table_tps = {tp for tp in tps
                     if tp.topic == table._changelog_topic_name()}
        self._sync_persisted_offsets(table, table_tps)
        tp_offsets: Counter[TP] = Counter({
            tp: offsets[tp]
            for tp in table_tps if tp in offsets
        })
        channel = self._channels[table]
        return ChangelogReader(
            table,
            channel,
            self.app,
            table_tps,
            tp_offsets,
            loop=self.loop,
            beacon=self.beacon,
        )

    def _sync_persisted_offsets(self, table: CollectionT,
                                tps: Set[TP]) -> None:
        for tp in tps:
            persisted_offset = table.persisted_offset(tp)
            if persisted_offset is not None:
                curr_offset = self._table_offsets.get(tp, -1)
                self._table_offsets[tp] = max(curr_offset, persisted_offset)

    @Service.transitions_to(TABLEMAN_START_STANDBYS)
    async def _start_standbys(self, tps: Set[TP]) -> None:
        self.log.info('Attempting to start standbys')
        assert not self._standbys
        table_standby_tps = self._group_table_tps(tps)
        offsets = self._table_offsets
        for table, table_tps in table_standby_tps.items():
            self.log.info('Starting standbys for tps: %s', tps)
            self._sync_persisted_offsets(table, table_tps)
            tp_offsets: Counter[TP] = Counter({
                tp: offsets[tp]
                for tp in table_tps if tp in offsets
            })
            channel = self._channels[table]
            standby = StandbyReader(
                table,
                channel,
                self.app,
                table_tps,
                tp_offsets,
                loop=self.loop,
                beacon=self.beacon,
            )
            self._standbys[table] = standby
            await standby.start()

    def _group_table_tps(self, tps: Set[TP]) -> CollectionTps:
        table_tps: CollectionTps = defaultdict(set)
        for tp in tps:
            if self._is_changelog_tp(tp):
                table_tps[self._changelogs[tp.topic]].add(tp)
        return table_tps

    def _is_changelog_tp(self, tp: TP) -> bool:
        return tp.topic in self.changelog_topics
