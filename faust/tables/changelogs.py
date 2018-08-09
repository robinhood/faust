import asyncio
from typing import (
    Any,
    AsyncIterable,
    Iterable,
    List,
    MutableMapping,
    Set,
    Tuple,
)

from mode import Service
from mode.utils.compat import Counter
from mode.utils.aiter import aenumerate
from mode.utils.times import Seconds

from faust.types import AppT, ChannelT, EventT, TP
from faust.types.tables import ChangelogReaderT, CollectionT
from faust.utils import terminal

__all__ = ['ChangelogReader', 'StandbyReader', 'local_tps']

CHANGELOG_SEEKING = 'SEEKING'
CHANGELOG_STARTING = 'STARTING'
CHANGELOG_READING = 'READING'


async def local_tps(table: CollectionT, tps: Iterable[TP]) -> Set[TP]:
    # RocksDB: Find partitions that we have database files for,
    # since only one process can have them open at a time.
    return {tp for tp in tps if not await table.need_active_standby_for(tp)}


class ChangelogReader(Service, ChangelogReaderT):
    """Service synchronizing table state from changelog topic."""

    wait_for_shutdown = True
    shutdown_timeout = None

    _highwaters: Counter[TP]
    _stop_event: asyncio.Event

    def __init__(self,
                 table: CollectionT,
                 channel: ChannelT,
                 app: AppT,
                 tps: Set[TP],
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

    def _repr_info(self) -> str:
        return f'{self.table!r} {self._stop_event!r}'

    @property
    def _buffer_size(self) -> int:
        return self.table.recovery_buffer_size

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
        table = terminal.logtable(
            [[k.topic, str(k.partition), str(v)]
             for k, v in self._highwaters.items()],
            title='Highwater',
            headers=['topic', 'partition', 'highwater'],
        )
        self.log.info('Highwater for changelog partitions:\n%s', table)

    def _should_stop_reading(self) -> bool:
        return not self._remaining_total()

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
        table = terminal.logtable(
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
            if offset >= 0:
                # FIXME Remove check when fixed offset-1 discrepancy
                await consumer.seek(tp, offset)
                assert await consumer.position(tp) == offset

    def _should_start_reading(self) -> bool:
        return self._highwaters != self.offsets

    async def wait_done_reading(self) -> None:
        # XXX [asksol] This method is unused, see note in tables/manager.py
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

    async def on_start(self) -> None:
        consumer = self.app.consumer
        await consumer.pause_partitions(self.tps)
        await self._build_highwaters()
        await self._update_offsets()
        if not self._should_start_reading():
            self.log.info('No updates needed')
            return self._done_reading()

        tps = await local_tps(self.table, self.tps)
        if tps:
            self.log.info('Partitions %r are local to this node', sorted(tps))
        # remove local tps from set of assigned partitions.
        self.tps.difference_update(tps)
        if not self.tps:
            self.log.info('No active standby needed')
            return self._done_reading()

        await self._seek_tps()
        await consumer.resume_partitions(self.tps)

    @Service.task
    @Service.transitions_to(CHANGELOG_STARTING)
    async def _read(self) -> None:
        if self.should_stop or self._shutdown.is_set():
            return
        # We don't want to log when there are zero records,
        # but we still slurp the stream so that we subscribe
        # to the changelog topic etc.
        if self._remaining_total():
            self.log.info('Reading %s records...', self._remaining_total())
        # log statement above, or the sleep below ...
        # fixes weird aiokafka 100% CPU deadlock [ask].
        await self.sleep(0.1)
        self.diag.set_flag(CHANGELOG_READING)
        try:
            await self._slurp_stream()
        finally:
            self.diag.unset_flag(CHANGELOG_READING)
            self._done_reading()

    async def on_stop(self) -> None:
        # Pause all our topic partitions,
        # to make sure we don't fetch any more records from them.
        assignment = self.app.consumer.assignment()
        await self.app.consumer.pause_partitions(
            {tp for tp in self.tps if tp in assignment})

        # Tell _read coroutine iterating over the changelog channel
        # that they should stop iterating.
        if not self._stop_event.is_set():
            await self.channel.throw(StopAsyncIteration())

    async def _slurp_stream(self) -> None:
        buf: List[EventT] = []
        can_log_done = True
        try:
            async for i, event in aenumerate(self._read_changelog()):
                buf.append(event)
                await self.table.on_changelog_event(event)
                if len(buf) >= self._buffer_size:
                    self.table.apply_changelog_batch(buf)
                    buf.clear()
                if self._should_stop_reading():
                    break
                remaining = self._remaining_total()
                if remaining and not i % 10_000:
                    can_log_done = True
                    self.log.info('Waiting for %s records...', remaining)
                elif not remaining and can_log_done:
                    can_log_done = False
                    self.log.info('All up to date')
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

    @property
    def _buffer_size(self) -> int:
        return self.table.standby_buffer_size

    @Service.task
    async def _publish_stats(self) -> None:
        return

    def _should_start_reading(self) -> bool:
        return True

    def _should_stop_reading(self) -> bool:
        return self.should_stop

    def recovered(self) -> bool:
        return False
