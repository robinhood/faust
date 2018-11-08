import asyncio
import typing
from collections import defaultdict
from typing import Any, List, MutableMapping, Optional, Set, Tuple, cast

from mode import Service
from mode.services import WaitArgT
from mode.utils.compat import Counter
from mode.utils.locks import Event

from faust.types import AppT, EventT, TP
from faust.types.tables import CollectionT, TableManagerT
from faust.types.transports import ConsumerT
from faust.utils import terminal

if typing.TYPE_CHECKING:
    from .manager import TableManager
else:
    class TableManager: ...  # noqa


class ServiceStopped(Exception):
    ...


class RebalanceAgain(Exception):
    ...


class Recovery(Service):

    app: AppT

    tables: TableManager

    stats_interval: float = 5.0

    #: Set of standby tps.
    standby_tps: Set[TP]

    #: Set of active tps.
    active_tps: Set[TP]

    #: Mapping from TP to table
    tp_to_table: MutableMapping[TP, CollectionT]

    #: Active offset by TP.
    active_offsets: Counter[TP]

    #: Standby offset by TP.
    standby_offsets: Counter[TP]

    #: Mapping of highwaters by tp.
    highwaters: Counter[TP]

    #: Active highwaters by TP.
    active_highwaters: Counter[TP]

    #: Standby highwaters by TP.
    standby_highwaters: Counter[TP]

    _signal_recovery_start: Optional[Event] = None
    _signal_recovery_end: Optional[Event] = None
    _signal_recovery_reset: Optional[Event] = None

    completed: Event
    in_recovery: bool = False
    recovery_delay: float

    #: Changelog event buffers by table.
    #: These are filled by background task `_slurp_changelog`,
    #: and need to be flushed before starting new recovery/stopping.
    buffers: MutableMapping[CollectionT, List[EventT]]

    #: Cache of buffer size by TopicPartitiojn.
    buffer_sizes: MutableMapping[TP, int]

    def __init__(self,
                 app: AppT,
                 tables: TableManagerT,
                 **kwargs: Any) -> None:
        self.app = app
        self.tables = cast(TableManager, tables)

        self.standby_tps = set()
        self.active_tps = set()

        self.tp_to_table = {}
        self.active_offsets = Counter()
        self.standby_offsets = Counter()

        self.active_highwaters = Counter()
        self.standby_highwaters = Counter()
        self.completed = Event()

        self.buffers = defaultdict(list)
        self.buffer_sizes = {}
        self.recovery_delay = self.app.conf.stream_recovery_delay

        super().__init__(**kwargs)

    @property
    def signal_recovery_start(self) -> Event:
        if self._signal_recovery_start is None:
            self._signal_recovery_start = Event(loop=self.loop)
        return self._signal_recovery_start

    @property
    def signal_recovery_end(self) -> Event:
        if self._signal_recovery_end is None:
            self._signal_recovery_end = Event(loop=self.loop)
        return self._signal_recovery_end

    @property
    def signal_recovery_reset(self) -> Event:
        if self._signal_recovery_reset is None:
            self._signal_recovery_reset = Event(loop=self.loop)
        return self._signal_recovery_reset

    async def on_stop(self) -> None:
        # Flush buffers when stopping.
        self.flush_buffers()

    def add_active(self, table: CollectionT, tp: TP) -> None:
        self.active_tps.add(tp)
        self._add(table, tp, self.active_offsets)

    def add_standby(self, table: CollectionT, tp: TP) -> None:
        table = self.tables._changelogs[tp.topic]
        self.standby_tps.add(tp)
        self._add(table, tp, self.standby_offsets)

    def _add(self, table: CollectionT, tp: TP, offsets: Counter[TP]) -> None:
        self.tp_to_table[tp] = table
        persisted_offset = table.persisted_offset(tp)
        if persisted_offset is not None:
            offsets[tp] = persisted_offset
        offsets.setdefault(tp, -1)

    def revoke(self, tp: TP) -> None:
        self.standby_offsets.pop(tp, None)
        self.standby_highwaters.pop(tp, None)
        self.active_offsets.pop(tp, None)
        self.active_highwaters.pop(tp, None)

    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        self.flush_buffers()
        self.signal_recovery_reset.set()

    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        assigned_standbys = self.app.assignor.assigned_standbys()
        assigned_actives = self.app.assignor.assigned_actives()

        for tp in revoked:
            self.revoke(tp)

        self.standby_tps.clear()
        self.active_tps.clear()

        for tp in assigned_standbys:
            table = self.tables._changelogs.get(tp.topic)
            if table is not None:
                self.add_standby(table, tp)
        for tp in assigned_actives:
            table = self.tables._changelogs.get(tp.topic)
            if table is not None:
                self.add_active(table, tp)

        active_offsets = {
            tp: offset
            for tp, offset in self.active_offsets.items()
            if tp in self.active_tps
        }
        self.active_offsets.clear()
        self.active_offsets.update(active_offsets)

        self.signal_recovery_reset.clear()
        self.signal_recovery_start.set()

    async def _resume_streams(self) -> None:
        app = self.app
        consumer = app.consumer
        await app.on_rebalance_complete.send()
        # Resume partitions and start fetching.
        self.log.info('Resuming flow...')
        consumer.resume_flow()
        app.flow_control.resume()
        self.log.info('Seek stream partitions to committed offsets.')
        await self._wait(consumer.perform_seek())
        self.completed.set()
        assignment = consumer.assignment()
        self.log.dev('Resume stream partitions')
        consumer.resume_partitions(assignment)
        # finally make sure the fetcher is running.
        await app._fetcher.maybe_start()
        app.rebalancing = False
        self.log.info('Worker ready')

    @Service.task
    async def _restart_recovery(self) -> None:
        consumer = self.app.consumer
        active_tps = self.active_tps
        standby_tps = self.standby_tps
        standby_offsets = self.standby_offsets
        standby_highwaters = self.standby_highwaters
        assigned_active_tps = self.active_tps
        assigned_standby_tps = self.standby_tps
        active_offsets = self.active_offsets
        standby_offsets = self.standby_offsets
        active_highwaters = self.active_highwaters

        while not self.should_stop:
            self.log.dev('WAITING FOR NEXT RECOVERY TO START')
            self.signal_recovery_reset.clear()
            self.in_recovery = False
            if await self.wait_for_stopped(self.signal_recovery_start):
                self.signal_recovery_start.clear()
                break  # service was stopped
            self.signal_recovery_start.clear()

            try:
                await self._wait(asyncio.sleep(self.recovery_delay))

                if not self.tables:
                    # If there are no tables -- simply resume streams
                    await self._resume_streams()
                    continue

                self.in_recovery = True
                # Must flush any buffers before starting rebalance.
                self.flush_buffers()
                await self._wait(self.app._producer.flush())

                self.log.dev('Build highwaters for active partitions')
                await self._wait(self._build_highwaters(
                    consumer, assigned_active_tps,
                    active_highwaters, 'active'))

                self.log.dev('Build offsets for active partitions')
                await self._wait(self._build_offsets(
                    consumer, assigned_active_tps, active_offsets, 'active'))

                self.log.dev('Build offsets for standby partitions')
                await self._wait(self._build_offsets(
                    consumer, assigned_standby_tps,
                    standby_offsets, 'standby'))

                self.log.dev('Seek offsets for active partitions')
                await self._wait(self._seek_offsets(
                    consumer, assigned_active_tps, active_offsets, 'active'))

                if self.need_recovery():
                    self.log.info('Restoring state from changelog topics...')
                    consumer.resume_partitions(active_tps)
                    # Resume partitions and start fetching.
                    self.log.info('Resuming flow...')
                    consumer.resume_flow()
                    await self.app._fetcher.maybe_start()
                    self.app.flow_control.resume()

                    # Wait for actives to be up to date.
                    # This signal will be set by _slurp_changelogs
                    self.signal_recovery_end.clear()
                    await self._wait(self.signal_recovery_end)

                    # recovery done.
                    self.log.info('Done reading from changelog topics')
                    consumer.pause_partitions(active_tps)
                else:
                    self.log.info('Resuming flow...')
                    consumer.resume_flow()
                    self.app.flow_control.resume()

                self.log.info('Recovery complete')
                self.in_recovery = False

                if standby_tps:
                    self.log.info('Starting standby partitions...')

                    self.log.dev('Seek standby offsets')
                    await self._wait(
                        self._seek_offsets(
                            consumer, standby_tps, standby_offsets, 'standby'))

                    self.log.dev('Build standby highwaters')
                    await self._wait(
                        self._build_highwaters(
                            consumer,
                            standby_tps,
                            standby_highwaters,
                            'standby',
                        ),
                    )

                    self.log.dev('Resume standby partitions')
                    consumer.resume_partitions(standby_tps)

                # Pause all our topic partitions,
                # to make sure we don't fetch any more records from them.
                await self._wait(asyncio.sleep(0.1))  # still needed?
                await self._wait(self.on_recovery_completed())
            except RebalanceAgain:
                self.log.dev('RAISED REBALANCE AGAIN')
                continue  # another rebalance started
            except ServiceStopped:
                self.log.dev('RAISED SERVICE STOPPED')
                break  # service was stopped
            # restart - wait for next rebalance.
        self.in_recovery = False

    async def _wait(self, coro: WaitArgT) -> None:
        wait_result = await self.wait_first(
            coro,
            self.signal_recovery_reset,
            self.signal_recovery_start,
        )
        if wait_result.stopped:
            # service was stopped.
            raise ServiceStopped()
        elif self.signal_recovery_start in wait_result.done:
            # another rebalance started
            raise RebalanceAgain()
        elif self.signal_recovery_reset in wait_result.done:
            raise RebalanceAgain()
        else:
            return None

    async def on_recovery_completed(self) -> None:
        consumer = self.app.consumer
        self.log.info('Restore complete!')
        await self.app.on_rebalance_complete.send()
        # This needs to happen if all goes well
        callback_coros = []
        for table in self.tables.values():
            callback_coros.append(table.call_recover_callbacks())
        if callback_coros:
            await asyncio.wait(callback_coros)
        self.log.info('Seek stream partitions to committed offsets.')
        await consumer.perform_seek()
        self.completed.set()
        assignment = consumer.assignment()
        self.log.dev('Resume stream partitions')
        consumer.resume_partitions({
            tp for tp in assignment
            if not self._is_changelog_tp(tp)
        })
        # finally make sure the fetcher is running.
        await self.app._fetcher.maybe_start()
        self.app.rebalancing = False
        self.log.info('Worker ready')

    async def _build_highwaters(self,
                                consumer: ConsumerT,
                                tps: Set[TP],
                                destination: Counter[TP],
                                title: str) -> None:
        # -- Build highwater
        highwaters = await consumer.highwaters(*tps)
        highwaters = {
            # FIXME the -1 here is because of the way we commit offsets
            tp: value - 1
            for tp, value in highwaters.items()
        }
        table = terminal.logtable(
            [[k.topic, str(k.partition), str(v)]
             for k, v in highwaters.items()],
            title=f'Highwater - {title.capitalize()}',
            headers=['topic', 'partition', 'highwater'],
        )
        self.log.info(
            'Highwater for %s changelog partitions:\n%s', title, table)
        destination.clear()
        destination.update(highwaters)

    async def _build_offsets(self,
                             consumer: ConsumerT,
                             tps: Set[TP],
                             destination: Counter[TP],
                             title: str) -> None:
        # -- Update offsets
        # Offsets may have been compacted, need to get to the recent ones
        earliest = await consumer.earliest_offsets(*tps)
        # FIXME To be consistent with the offset -1 logic
        earliest = {tp: offset - 1 for tp, offset in earliest.items()}
        for tp in tps:
            destination[tp] = max(destination[tp], earliest[tp])
        table = terminal.logtable(
            [(k.topic, k.partition, v) for k, v in destination.items()],
            title=f'Reading Starts At - {title.capitalize()}',
            headers=['topic', 'partition', 'offset'],
        )
        self.log.info('%s offsets at start of reading:\n%s', title, table)

    async def _seek_offsets(self,
                            consumer: ConsumerT,
                            tps: Set[TP],
                            offsets: Counter[TP],
                            title: str) -> None:
        # Seek to new offsets
        for tp in tps:
            offset = offsets[tp]
            if offset == -1:
                offset = 0
            # FIXME Remove check when fixed offset-1 discrepancy
            await consumer.seek(tp, offset)
            assert await consumer.position(tp) == offset

    @Service.task
    async def _slurp_changelogs(self) -> None:
        changelog_queue = self.tables.changelog_queue
        tp_to_table = self.tp_to_table

        active_tps = self.active_tps
        standby_tps = self.standby_tps
        active_offsets = self.active_offsets
        standby_offsets = self.standby_offsets

        buffers = self.buffers
        buffer_sizes = self.buffer_sizes

        while not self.should_stop:
            event: EventT = await changelog_queue.get()
            message = event.message
            tp = message.tp
            offset = message.offset

            offsets: Counter[TP]
            bufsize = buffer_sizes.get(tp)
            if tp in active_tps:
                table = tp_to_table[tp]
                offsets = active_offsets
                if bufsize is None:
                    bufsize = buffer_sizes[tp] = table.recovery_buffer_size
            elif tp in standby_tps:
                table = tp_to_table[tp]
                offsets = standby_offsets
                if bufsize is None:
                    bufsize = buffer_sizes[tp] = table.standby_buffer_size
            else:
                continue

            seen_offset = offsets.get(tp, -1)
            if offset > seen_offset:
                offsets[tp] = offset
                buf = buffers[table]
                buf.append(event)
                await table.on_changelog_event(event)
                if len(buf) >= bufsize:
                    table.apply_changelog_batch(buf)
                    buf.clear()
            if self.in_recovery and not self.active_remaining_total():
                # apply anything stuck in the buffers
                self.flush_buffers()
                self.in_recovery = False
                self.signal_recovery_end.set()

    def flush_buffers(self) -> None:
        for table, buffer in self.buffers.items():
            table.apply_changelog_batch(buffer)
            buffer.clear()

    def need_recovery(self) -> bool:
        return self.active_highwaters != self.active_offsets

    def active_remaining(self) -> Counter[TP]:
        return self.active_highwaters - self.active_offsets

    def standby_remaining(self) -> Counter[TP]:
        return self.standby_highwaters - self.standby_offsets

    def active_remaining_total(self) -> int:
        return sum(self.active_remaining().values())

    def standby_remaining_total(self) -> int:
        return sum(self.standby_remaining().values())

    def active_stats(self) -> MutableMapping[TP, Tuple[int, int, int]]:
        offsets = self.active_offsets
        return {
            tp: (highwater, offsets[tp], highwater - offsets[tp])
            for tp, highwater in self.active_highwaters.items()
            if highwater - offsets[tp] != 0
        }

    def standby_stats(self) -> MutableMapping[TP, Tuple[int, int, int]]:
        offsets = self.standby_offsets
        return {
            tp: (highwater, offsets[tp], highwater - offsets[tp])
            for tp, highwater in self.standby_highwaters.items()
            if highwater - offsets[tp] != 0
        }

    @Service.task
    async def _publish_stats(self) -> None:
        while not self.should_stop:
            if self.in_recovery:
                self.log.info(
                    'Still fetching. Remaining: %s', self.active_stats())
            await self.sleep(self.stats_interval)

    def _is_changelog_tp(self, tp: TP) -> bool:
        return tp.topic in self.tables.changelog_topics
