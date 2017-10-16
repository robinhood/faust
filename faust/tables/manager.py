"""Tables (changelog stream)."""
import asyncio
import typing
from collections import defaultdict
from typing import (
    Any, AsyncIterable, Iterable, List, MutableMapping, cast,
)
from mode import PoisonpillSupervisor, Service
from mode.utils.compat import Counter
from .table import Table
from ..types import AppT, EventT, TP
from ..types.tables import (
    ChangelogReaderT, CollectionT, CollectionTps, TableManagerT,
)
from ..types.topics import ChannelT
from ..utils.aiter import aenumerate, aiter
from ..utils.collections import FastUserDict

__all__ = [
    'ChangelogReader',
    'StandbyReader',
    'TableManager',
]

CHANGELOG_SEEKING = 'SEEKING'
CHANGELOG_STARTING = 'STARTING'
CHANGELOG_READING = 'READING'
TABLEMAN_UPDATE = 'UPDATE'
TABLEMAN_START_STANDBYS = 'START_STANDBYS'
TABLEMAN_STOP_STANDBYS = 'STOP_STANDBYS'
TABLEMAN_RECOVER = 'RECOVER'
TABLEMAN_PARTITIONS_REVOKED = 'PARTITIONS REVOKED'
TABLEMAN_PARTITIONS_ASSIGNED = 'PARTITIONS_ASSIGNED'


class ChangelogReader(Service, ChangelogReaderT):
    wait_for_shutdown = True
    shutdown_timeout = None

    _highwaters: Counter[TP] = None
    _stop_event: asyncio.Event = None

    def __init__(self, table: CollectionT,
                 channel: ChannelT,
                 app: AppT,
                 tps: Iterable[TP],
                 offsets: Counter[TP] = None,
                 stop_event: asyncio.Event = None,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.channel = channel
        self.app = app
        self.tps = tps
        self.offsets = Counter() if offsets is None else offsets
        for tp in self.tps:
            self.offsets.setdefault(tp, -1)
        self._highwaters = Counter()
        self._started_reading = asyncio.Event(loop=self.loop)
        self._stop_event = stop_event

    async def on_started(self) -> None:
        # We wait for the background task to start reading
        # before considering this service to be started.
        await self.wait(self._started_reading.wait())

    async def _build_highwaters(self) -> None:
        consumer = self.app.consumer
        tps = self.tps
        highwaters = await consumer.highwaters(*tps)
        self._highwaters.clear()
        self._highwaters.update({
            # FIXME the -1 here is because of the way we commit offsets
            tp: highwaters[tp] - 1
            for tp in tps
        })

    def _should_stop_reading(self) -> bool:
        return self._highwaters == self.offsets

    def _remaining(self) -> Counter[TP]:
        return self._highwaters - self.offsets

    def _remaining_total(self) -> int:
        return sum(self._remaining().values())

    @Service.transitions_to(CHANGELOG_SEEKING)
    async def _seek_tps(self) -> None:
        consumer = self.app.consumer
        tps = self.tps
        for tp in tps:
            offset = max(self.offsets[tp], 0)
            self.log.info(f'Seeking {tp} to offset: {offset}')
            await consumer.seek(tp, offset)
            assert await consumer.position(tp) == offset

    def _should_start_reading(self) -> bool:
        return self._highwaters != self.offsets

    @Service.task
    @Service.transitions_to(CHANGELOG_STARTING)
    async def _read(self) -> None:
        print('READER STARTING: %r' % (self,))
        table = self.table
        consumer = self.app.consumer
        await consumer.pause_partitions(self.tps)
        await self._build_highwaters()
        if not self._should_start_reading():
            self.log.info('No updates needed')
            self._started_reading.set()
            self.set_shutdown()
            return

        # RocksDB: Find partitions that we have database files for,
        # since only one process can have them open at a time.
        local_tps = {
            tp for tp in self.tps
            if not await table.need_active_standby_for(tp)
        }
        if local_tps:
            self.log.info('Partitions %r are local to this node',
                          sorted(local_tps))

        self.tps = list(set(self.tps) - local_tps)
        if not self.tps:
            self.log.info('No active standby needed')
            self._started_reading.set()
            self.set_shutdown()
            return

        await self._seek_tps()
        await consumer.resume_partitions(self.tps)
        self.log.info(f'Reading %s records...', self._remaining_total())
        self._started_reading.set()
        buf: List[EventT] = []
        self.diag.set_flag(CHANGELOG_READING)
        try:
            async for i, event in aenumerate(self._read_changelog()):
                buf.append(event)
                if self._stop_event is not None and self._stop_event.is_set():
                    self.log.info('Recovery aborted')
                    break
                if len(buf) >= 1000:
                    table.apply_changelog_batch(buf)
                    buf.clear()
                if self._should_stop_reading():
                    break
                if not i % 10_000:
                    self.log.info('Still waiting for %s records...',
                                  self._remaining_total())
        except StopAsyncIteration:
            pass
        finally:
            self.diag.unset_flag(CHANGELOG_READING)
            if buf:
                table.apply_changelog_batch(buf)
                buf.clear()
            pause_tps = {tp for tp in self.tps if tp in consumer.assignment()}
            await consumer.pause_partitions(pause_tps)
            self.set_shutdown()

    async def _read_changelog(self) -> AsyncIterable[EventT]:
        offsets = self.offsets

        async for event in self.channel:
            message = event.message
            tp = message.tp
            offset = message.offset
            seen_offset = offsets.get(tp, -1)
            if offset > seen_offset:
                offsets[tp] = offset
                yield event

    @property
    def label(self) -> str:
        return self.shortlabel

    @property
    def shortlabel(self) -> str:
        return f'{type(self).__name__}: {self.table.name}'


class StandbyReader(ChangelogReader):

    async def on_stop(self) -> None:
        await self.channel.throw(StopAsyncIteration())

    def _should_start_reading(self) -> bool:
        return True

    def _should_stop_reading(self) -> bool:
        return self.should_stop


class TableManager(Service, TableManagerT, FastUserDict):

    _channels: MutableMapping[CollectionT, ChannelT]
    _changelogs: MutableMapping[str, CollectionT]
    _table_offsets: Counter[TP]
    _standbys: MutableMapping[CollectionT, ChangelogReaderT]
    _ongoing_recovery: asyncio.Future = None
    _stop_recovery: asyncio.Event = None
    _recovery_started: asyncio.Event
    recovery_completed: asyncio.Event

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.data = {}
        self._channels = {}
        self._changelogs = {}
        self._table_offsets = Counter()
        self._standbys = {}
        self._recovery_started = asyncio.Event(loop=self.loop)
        self.recovery_completed = asyncio.Event(loop=self.loop)

    def __hash__(self) -> int:
        return object.__hash__(self)

    @property
    def changelog_topics(self) -> typing.Set[str]:
        return set(self._changelogs.keys())

    def add(self, table: CollectionT) -> CollectionT:
        if self._recovery_started.is_set():
            raise RuntimeError('Too late to add tables at this point')
        assert table.name is not None
        if table.name in self:
            raise ValueError(f'Table with name {table.name!r} already exists')
        self[table.name] = table
        return table

    @Service.transitions_to(TABLEMAN_UPDATE)
    async def _update_channels(self) -> None:
        for table in self.values():
            if table not in self._channels:
                self._channels[table] = cast(ChannelT, aiter(
                    table.changelog_topic))
        self._changelogs.update({
            table.changelog_topic.get_topic_name(): table
            for table in self.values()
        })
        await self.app.consumer.pause_partitions({
            tp for tp in self.app.consumer.assignment()
            if tp.topic in self._changelogs
        })

    def _sync_persisted_offsets(self,
                                table: CollectionT,
                                tps: Iterable[TP]) -> None:
        for tp in tps:
            persisted_offset = table.persisted_offset(tp)
            if persisted_offset is not None:
                curr_offset = self._table_offsets.get(tp, -1)
                self._table_offsets[tp] = max(curr_offset, persisted_offset)

    def _sync_offsets(self, reader: ChangelogReaderT) -> None:
        self.log.info(f'Syncing offsets {reader.offsets}')
        # We do counter union as new offsets should be >= old offsets
        self._table_offsets = self._table_offsets | reader.offsets

    @Service.transitions_to(TABLEMAN_STOP_STANDBYS)
    async def _stop_standbys(self) -> None:
        for _, standby in self._standbys.items():
            self.log.info(f'Stopping standby for tps: {standby.tps}')
            await standby.stop()
            self._sync_offsets(standby)
        self._standbys = {}

    def _group_table_tps(self, tps: Iterable[TP]) -> CollectionTps:
        table_tps: CollectionTps = defaultdict(list)
        for tp in tps:
            if self._is_changelog_tp(tp):
                table_tps[self._changelogs[tp.topic]].append(tp)
        return table_tps

    @Service.transitions_to(TABLEMAN_START_STANDBYS)
    async def _start_standbys(self,
                              tps: Iterable[TP]) -> None:
        self.log.info('Attempting to start standbys')
        assert not self._standbys
        table_standby_tps = self._group_table_tps(tps)
        offsets = self._table_offsets
        for table, tps in table_standby_tps.items():
            self.log.info(f'Starting standbys for tps: {tps}')
            self._sync_persisted_offsets(table, tps)
            tp_offsets: Counter[TP] = Counter({
                tp: offsets[tp]
                for tp in tps if tp in offsets
            })
            channel = self._channels[table]
            standby = StandbyReader(
                table, channel, self.app, tps, tp_offsets,
                loop=self.loop,
                beacon=self.beacon,
            )
            self._standbys[table] = standby
            await standby.start()

    def _is_changelog_tp(self, tp: TP) -> bool:
        return tp.topic in self.changelog_topics

    async def _on_recovery_started(self) -> None:
        self._recovery_started.set()
        await self._update_channels()

    async def _on_recovery_completed(self) -> None:
        for table in self.values():
            await table.maybe_start()
        self.recovery_completed.set()

    async def on_start(self) -> None:
        await self.sleep(1.0)
        await self._update_channels()

    async def on_stop(self) -> None:
        if self.recovery_completed.is_set():
            for table in self.values():
                await table.stop()

    @Service.transitions_to(TABLEMAN_RECOVER)
    async def _recover_changelogs(self, tps: Iterable[TP],
                                  stop_event: asyncio.Event) -> None:
        self.log.info('Recovering from changelog topics...')
        table_recoverers: List[ChangelogReaderT] = [
            self._create_recoverer(table, tps, stop_event)
            for table in self.values()
        ]
        supervisor = PoisonpillSupervisor(*table_recoverers,
                                          loop=self.loop, beacon=self.beacon)
        await supervisor.start()
        await supervisor.stop()
        for recoverer in table_recoverers:
            self._sync_offsets(recoverer)
        self.log.info('Done recovering from changelog topics')

    def _create_recoverer(self,
                          table: CollectionT,
                          tps: Iterable[TP],
                          stop_event: asyncio.Event) -> ChangelogReaderT:
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
            table, channel, self.app, table_tps, tp_offsets,
            loop=self.loop,
            beacon=self.beacon,
            stop_event=stop_event,
        )

    async def _recover(self, assigned: Iterable[TP]) -> None:
        standby_tps = self.app.assignor.assigned_standbys()
        assigned_tps = self.app.assignor.assigned_actives()
        assert set(assigned_tps).issubset(set(assigned))
        self.log.info('New assignments found')
        # This needs to happen in background and be aborted midway
        await self._on_recovery_started()
        for table in self.values():
            await table.on_partitions_assigned(assigned)
        assert self._stop_recovery is None
        stop_recovery = self._stop_recovery = asyncio.Event(loop=self.loop)
        await self._recover_changelogs(assigned_tps, stop_recovery)
        if not stop_recovery.is_set():
            # This needs to happen if all goes well
            recover_callback_coros = [
                table.on_recovery() for table in self.values()
            ]
            if recover_callback_coros:
                await asyncio.wait(recover_callback_coros)
            await self.app.consumer.resume_partitions({
                tp for tp in assigned
                if not self._is_changelog_tp(tp)
            })
            await self._start_standbys(standby_tps)
            self.log.info('New assignments handled')
            await self._on_recovery_completed()

    async def _maybe_abort_ongoing_recovery(self) -> None:
        self.log.info('Aborting ongoing recovery')
        if self._ongoing_recovery is None:
            return
        if not self._ongoing_recovery.done():
            self._stop_recovery.set()
            await self.wait(self._ongoing_recovery)
        self._ongoing_recovery = None
        self._stop_recovery = None

    @Service.transitions_to(TABLEMAN_PARTITIONS_REVOKED)
    async def on_partitions_revoked(self, revoked: Iterable[TP]) -> None:
        await self._maybe_abort_ongoing_recovery()
        self.log.info('Attempting to stop standbys')
        await self._stop_standbys()
        for table in self.values():
            await table.on_partitions_revoked(revoked)

    @Service.transitions_to(TABLEMAN_PARTITIONS_ASSIGNED)
    async def on_partitions_assigned(self, assigned: Iterable[TP]) -> None:
        assert self._ongoing_recovery is None and self._stop_recovery is None
        self._ongoing_recovery = self.add_future(self._recover(assigned))
        self.log.info('Triggered recovery in background')


__flake8_List_is_used: List  # XXX flake8 bug
