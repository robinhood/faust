"""Tables (changelog stream)."""
import asyncio
import typing
from collections import defaultdict
from typing import (
    Any, AsyncIterable, Iterable, List, MutableMapping, Set, Tuple, cast,
)
from mode import Service
from mode.utils.compat import Counter
from mode.utils.times import Seconds
from .table import Table
from ..types import AppT, EventT, TP
from ..types.tables import (
    ChangelogReaderT, CollectionT, CollectionTps, TableManagerT,
)
from ..types.topics import ChannelT
from ..utils import text
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


async def _local_tps(table: CollectionT, tps: Iterable[TP]) -> Set[TP]:
    # RocksDB: Find partitions that we have database files for,
    # since only one process can have them open at a time.
    return {
        tp for tp in tps
        if not await table.need_active_standby_for(tp)
    }


class ChangelogReader(Service, ChangelogReaderT):
    """Service synchronizing table state from changelog topic."""

    wait_for_shutdown = True
    shutdown_timeout = None

    _highwaters: Counter[TP] = None
    _stop_event: asyncio.Event = None

    def __init__(self, table: CollectionT,
                 channel: ChannelT,
                 app: AppT,
                 tps: Iterable[TP],
                 offsets: Counter[TP] = None,
                 stats_interval: Seconds = 5.0,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.channel = channel
        self.app = app
        self.tps = tps
        self.offsets = Counter() if offsets is None else offsets
        self.stats_interval = stats_interval
        for tp in self.tps:
            self.offsets.setdefault(tp, -1)
        self._highwaters = Counter()
        self._stop_event = asyncio.Event(loop=self.loop)

    async def on_stop(self) -> None:
        # Stop reading changelog
        if not self._stop_event.is_set():
            await self.channel.throw(StopAsyncIteration())

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
        table = text.logtable(
            [(k.topic, k.partition, v) for k, v in self._highwaters.items()],
            title='Highwater',
            headers=['topic', 'partition', 'highwater'],
        )
        self.log.info('Highwater for changelog partitions:\n%s', table)

    def _should_stop_reading(self) -> bool:
        return self._highwaters == self.offsets

    def _remaining(self) -> Counter[TP]:
        return self._highwaters - self.offsets

    def _remaining_total(self) -> int:
        return sum(self._remaining().values())

    async def _update_offsets(self) -> None:
        # Offsets may have been compacted, need to get to the recent ones
        consumer = self.app.consumer
        earliest = await consumer.earliest_offsets(*self.tps)
        # FIXME: To be consistent with the offset -1 logic
        earliest = {tp: offset - 1 for tp, offset in earliest.items()}
        for tp in self.tps:
            self.offsets[tp] = max(self.offsets[tp], earliest[tp])
        table = text.logtable(
            [(k.topic, k.partition, v) for k, v in self.offsets.items()],
            title='Reading Starts At',
            headers=['topic', 'partition', 'offset'],
        )
        self.log.info('Updated offsets at start of reading:\n%s', table)

    @Service.transitions_to(CHANGELOG_SEEKING)
    async def _seek_tps(self) -> None:
        consumer = self.app.consumer
        tps = self.tps
        for tp in tps:
            offset = self.offsets[tp]
            # self.log.info(f'Seeking {tp} to offset: {offset}')
            if offset >= 0:
                # FIXME: Remove check when fixed offset-1 discrepancy
                await consumer.seek(tp, offset)
                assert await consumer.position(tp) == offset

    def _should_start_reading(self) -> bool:
        return self._highwaters != self.offsets

    async def wait_done_reading(self) -> None:
        await self._stop_event.wait()

    def _done_reading(self) -> None:
        self.set_shutdown()
        self._stop_event.set()
        self.log.info('Setting stop event')

    @property
    def _remaining_stats(self) -> MutableMapping[TP, Tuple[int, int, int]]:
        offsets = self.offsets
        return {
            tp: (highwater, offsets[tp], highwater - offsets[tp])
            for tp, highwater in self._highwaters.items()
            if highwater - offsets[tp] != 0
        }

    def recovered(self) -> bool:
        did_recover = self._highwaters == self.offsets
        if not did_recover:
            self.log.info('Did not recover. Remaining %s',
                          self._remaining_stats)
        return did_recover

    @Service.task
    async def _publish_stats(self) -> None:
        while not self.should_stop and not self._stop_event.is_set():
            self.log.info('Still fetching. Remaining: %s',
                          self._remaining_stats)
            await self.sleep(self.stats_interval)

    @Service.task
    @Service.transitions_to(CHANGELOG_STARTING)
    async def _read(self) -> None:
        consumer = self.app.consumer
        await consumer.pause_partitions(self.tps)
        await self._build_highwaters()
        await self._update_offsets()
        if not self._should_start_reading():
            self.log.info('No updates needed')
            return self._done_reading()

        local_tps = await _local_tps(self.table, self.tps)
        if local_tps:
            self.log.info('Partitions %r are local to this node',
                          sorted(local_tps))
        self.tps = list(set(self.tps) - local_tps)
        if not self.tps:
            self.log.info('No active standby needed')
            return self._done_reading()

        await self._seek_tps()
        await consumer.resume_partitions(self.tps)
        self.log.info('Reading %s records...', self._remaining_total())
        self.diag.set_flag(CHANGELOG_READING)
        try:
            await self._slurp_stream()
        finally:
            self.diag.unset_flag(CHANGELOG_READING)
            self._done_reading()
            await self.app.consumer.pause_partitions({
                tp for tp in self.tps
                if tp in self.app.consumer.assignment()
            })

    async def _slurp_stream(self) -> None:
        buf: List[EventT] = []
        try:
            async for i, event in aenumerate(self._read_changelog()):
                buf.append(event)
                await self.table.on_changelog_event(event)
                if len(buf) >= self.table.recovery_buffer_size:
                    self.table.apply_changelog_batch(buf)
                    buf.clear()
                if self._should_stop_reading():
                    break
                if not i % 10_000:
                    self.log.info('Still waiting for %s records...',
                                  self._remaining_total())
        except StopAsyncIteration:
            self.log.info('Got stop iteration')
            pass
        finally:
            self.log.info('Stopped reading!')
            if buf:
                self.table.apply_changelog_batch(buf)
                buf.clear()

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
    """Service reading table changelogs to keep an up-to-date backup."""

    @Service.task
    async def _publish_stats(self) -> None:
        return

    def _should_start_reading(self) -> bool:
        return True

    def _should_stop_reading(self) -> bool:
        return self.should_stop

    def recovered(self) -> bool:
        return False


class TableManager(Service, TableManagerT, FastUserDict):
    """Manage tables used by Faust worker."""

    _channels: MutableMapping[CollectionT, ChannelT]
    _changelogs: MutableMapping[str, CollectionT]
    _table_offsets: Counter[TP]
    _standbys: MutableMapping[CollectionT, ChangelogReaderT]
    _recoverers: List[ChangelogReaderT] = None
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
        table = text.logtable(
            [(k.topic, k.partition, v) for k, v in reader.offsets.items()],
            title='Sync Offset',
            headers=['topic', 'partition', 'offset'],
        )
        self.log.info('Syncing offsets:\n%s', table)
        for tp, offset in reader.offsets.items():
            if offset >= 0:
                table_offset = self._table_offsets.get(tp, -1)
                self._table_offsets[tp] = max(table_offset, offset)
        table = text.logtable(
            [(k.topic, k.partition, v)
             for k, v in self._table_offsets.items()],
            title='Table Offsets',
            headers=['topic', 'partition', 'offset'],
        )
        self.log.info('After syncing:\n%s', table)

    @Service.transitions_to(TABLEMAN_STOP_STANDBYS)
    async def _stop_standbys(self) -> None:
        for standby in self._standbys.values():
            self.log.info('Stopping standby for tps: %s', standby.tps)
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
            self.log.info('Starting standbys for tps: %s', tps)
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
        await self._maybe_abort_ongoing_recovery()
        await self._stop_standbys()
        for table in self.values():
            await table.stop()

    @Service.transitions_to(TABLEMAN_RECOVER)
    async def _recover_changelogs(self, tps: Iterable[TP]) -> bool:
        self.log.info('Recovering from changelog topics...')
        table_recoverers = self._recoverers = [
            self._create_recoverer(table, tps)
            for table in self.values()
        ]
        for recoverer in table_recoverers:
            await recoverer.start()
            self.log.info('Started recoverer: %s', recoverer.label)
        self.log.info('Waiting for recoverers to finish...')
        await asyncio.gather(
            *[r.wait_done_reading() for r in table_recoverers],
            loop=self.loop,
        )
        self.log.info('Done reading all changelogs')
        for recoverer in table_recoverers:
            self._sync_offsets(recoverer)
        self.log.info('Done reading from changelog topics')
        for recoverer in table_recoverers:
            await recoverer.stop()
            self.log.info('Stopped recoverer: %s', recoverer.label)
        self.log.info('Stopped all recoveres')
        return all(recoverer.recovered()
                   for recoverer in table_recoverers)

    def _create_recoverer(self,
                          table: CollectionT,
                          tps: Iterable[TP]) -> ChangelogReaderT:
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
        )

    async def _recover(self, assigned: Iterable[TP]) -> None:
        standby_tps = self.app.assignor.assigned_standbys()
        # for table in self.values():
        #     standby_tps = await _local_tps(table, standby_tps)
        assigned_tps = self.app.assignor.assigned_actives()
        assert set(assigned_tps).issubset(set(assigned))
        self.log.info('New assignments found')
        # This needs to happen in background and be aborted midway
        await self._on_recovery_started()
        for table in self.values():
            await table.on_partitions_assigned(assigned)
        did_recover = await self._recover_changelogs(assigned_tps)

        if did_recover and not self._stopped.is_set():
            self.log.info('Recovered fine!')
            # This needs to happen if all goes well
            callback_coros = [table.call_recover_callbacks()
                              for table in self.values()]
            if callback_coros:
                await asyncio.wait(callback_coros)
            await self.app.consumer.resume_partitions({
                tp for tp in assigned
                if not self._is_changelog_tp(tp)
            })
            await self._start_standbys(standby_tps)
            self.log.info('New assignments handled')
            await self._on_recovery_completed()
        else:
            self.log.info('Recovery interrupted')
        self._recoverers = None

    async def _maybe_abort_ongoing_recovery(self) -> None:
        if self._ongoing_recovery is not None:
            self.log.info('Aborting ongoing recovery')
            if not self._ongoing_recovery.done():
                assert self._recoverers is not None
                # TableManager.stop() will now block until all recoverers are
                # stopped. This is expected. Ideally the recoverers should stop
                # almost immediately upon receiving a stop()
                await asyncio.wait([recoverer.stop()
                                    for recoverer in self._recoverers])
                self.log.info('Waiting ongoing recovery')
                await self.wait(self._ongoing_recovery)
                self.log.info('Done with ongoing recovery')
            self._ongoing_recovery = None

    @Service.transitions_to(TABLEMAN_PARTITIONS_REVOKED)
    async def on_partitions_revoked(self, revoked: Iterable[TP]) -> None:
        await self._maybe_abort_ongoing_recovery()
        self.log.info('Aborted any ongoing recovery!')
        await self._stop_standbys()
        for table in self.values():
            await table.on_partitions_revoked(revoked)

    @Service.transitions_to(TABLEMAN_PARTITIONS_ASSIGNED)
    async def on_partitions_assigned(self, assigned: Iterable[TP]) -> None:
        assert self._ongoing_recovery is None and self._recoverers is None
        self._ongoing_recovery = self.add_future(self._recover(assigned))
        self.log.info('Triggered recovery in background')
