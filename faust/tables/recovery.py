"""Table recovery after rebalancing."""
import asyncio
import typing
from collections import defaultdict
from typing import Any, List, MutableMapping, Optional, Set, Tuple, cast

import opentracing
from mode import Service
from mode.services import WaitArgT
from mode.utils.locks import Event
from mode.utils.typing import Counter

from faust.exceptions import ConsistencyError
from faust.types import AppT, EventT, TP
from faust.types.tables import CollectionT, TableManagerT
from faust.types.transports import ConsumerT
from faust.utils import terminal
from faust.utils.tracing import finish_span, traced_from_parent_span

if typing.TYPE_CHECKING:
    from .manager import TableManager as _TableManager
else:
    class _TableManager: ...  # noqa

E_PERSISTED_OFFSET = '''\
The persisted offset for changelog topic partition {0} is higher
than the last offset in that topic (highwater) ({1} > {2}).

Most likely you have removed data from the topics without
removing the RocksDB database file for this partition.
'''


class ServiceStopped(Exception):
    """The recovery service was stopped."""


class RebalanceAgain(Exception):
    """During rebalance, another rebalance happened."""


class Recovery(Service):
    """Service responsible for recovering tables from changelog topics."""

    app: AppT

    tables: _TableManager

    stats_interval: float = 5.0

    #: Set of standby tps.
    standby_tps: Set[TP]

    #: Set of active tps.
    active_tps: Set[TP]

    actives_for_table: MutableMapping[CollectionT, Set[TP]]
    standbys_for_table: MutableMapping[CollectionT, Set[TP]]

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
    standbys_pending: bool = False
    recovery_delay: float

    #: Changelog event buffers by table.
    #: These are filled by background task `_slurp_changelog`,
    #: and need to be flushed before starting new recovery/stopping.
    buffers: MutableMapping[CollectionT, List[EventT]]

    #: Cache of buffer size by TopicPartitiojn.
    buffer_sizes: MutableMapping[TP, int]

    _recovery_span: Optional[opentracing.Span] = None
    _actives_span: Optional[opentracing.Span] = None
    _standbys_span: Optional[opentracing.Span] = None

    def __init__(self,
                 app: AppT,
                 tables: TableManagerT,
                 **kwargs: Any) -> None:
        self.app = app
        self.tables = cast(_TableManager, tables)

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

        self.actives_for_table = defaultdict(set)
        self.standbys_for_table = defaultdict(set)

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
        self.actives_for_table[table].add(tp)
        self._add(table, tp, self.active_offsets)

    def add_standby(self, table: CollectionT, tp: TP) -> None:
        self.standby_tps.add(tp)
        self.standbys_for_table[table].add(tp)
        self._add(table, tp, self.standby_offsets)

    def _add(self, table: CollectionT, tp: TP, offsets: Counter[TP]) -> None:
        self.tp_to_table[tp] = table
        persisted_offset = table.persisted_offset(tp)
        if persisted_offset is not None:
            offsets[tp] = persisted_offset
        offsets.setdefault(tp, None)

    def revoke(self, tp: TP) -> None:
        self.standby_offsets.pop(tp, None)
        self.standby_highwaters.pop(tp, None)
        self.active_offsets.pop(tp, None)
        self.active_highwaters.pop(tp, None)

    def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        T = traced_from_parent_span()
        T(self.flush_buffers)()
        self.signal_recovery_reset.set()

    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        app = self.app
        assigned_standbys = app.assignor.assigned_standbys()
        assigned_actives = app.assignor.assigned_actives()

        for tp in revoked:
            self.revoke(tp)

        self.standby_tps.clear()
        self.active_tps.clear()
        self.actives_for_table.clear()
        self.standbys_for_table.clear()

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

        if app.tracer and app._rebalancing_span:
            self._recovery_span = app.tracer.get_tracer('_faust').start_span(
                'recovery',
                child_of=app._rebalancing_span,
            )
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
        assignment = consumer.assignment()
        if assignment:
            self.log.info('Seek stream partitions to committed offsets.')
            await self._wait(consumer.perform_seek())
            self.log.dev('Resume stream partitions')
            consumer.resume_partitions(assignment)
        else:
            self.log.info('Resuming streams with empty assignment')
        self.completed.set()
        # finally make sure the fetcher is running.
        await app._fetcher.maybe_start()
        self.tables.on_actives_ready()
        self.tables.on_standbys_ready()
        app.on_rebalance_end()
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

            span: Any = None
            spans: list = []
            tracer: Optional[opentracing.Tracer] = None
            if self.app.tracer:
                tracer = self.app.tracer.get_tracer('_faust')
            if tracer is not None and self._recovery_span:
                span = tracer.start_span(
                    'recovery-thread',
                    child_of=self._recovery_span)
                spans.extend([span, self._recovery_span])
            T = traced_from_parent_span(span)

            try:
                await self._wait(T(asyncio.sleep)(self.recovery_delay))

                if not self.tables:
                    # If there are no tables -- simply resume streams
                    await T(self._resume_streams)()
                    for _span in spans:
                        finish_span(_span)
                    continue

                self.in_recovery = True
                self.standbys_pending = True
                # Must flush any buffers before starting rebalance.
                T(self.flush_buffers)()
                await self._wait(T(self.app._producer.flush)())

                self.log.dev('Build highwaters for active partitions')
                await self._wait(T(self._build_highwaters)(
                    consumer, assigned_active_tps,
                    active_highwaters, 'active'))

                self.log.dev('Build offsets for active partitions')
                await self._wait(T(self._build_offsets)(
                    consumer, assigned_active_tps, active_offsets, 'active'))

                for tp in assigned_active_tps:
                    if active_offsets[tp] > active_highwaters[tp]:
                        raise ConsistencyError(
                            E_PERSISTED_OFFSET.format(
                                tp,
                                active_offsets[tp],
                                active_highwaters[tp],
                            ),
                        )

                self.log.dev('Build offsets for standby partitions')
                await self._wait(T(self._build_offsets)(
                    consumer, assigned_standby_tps,
                    standby_offsets, 'standby'))

                self.log.dev('Seek offsets for active partitions')
                await self._wait(T(self._seek_offsets)(
                    consumer, assigned_active_tps, active_offsets, 'active'))

                if self.need_recovery():
                    self.log.info('Restoring state from changelog topics...')
                    T(consumer.resume_partitions)(active_tps)
                    # Resume partitions and start fetching.
                    self.log.info('Resuming flow...')
                    T(consumer.resume_flow)()
                    await T(self.app._fetcher.maybe_start)()
                    T(self.app.flow_control.resume)()

                    # Wait for actives to be up to date.
                    # This signal will be set by _slurp_changelogs
                    if tracer is not None and span:
                        self._actives_span = tracer.start_span(
                            'recovery-actives',
                            child_of=span,
                            tags={'Active-Stats': self.active_stats()},
                        )
                    try:
                        self.signal_recovery_end.clear()
                        await self._wait(self.signal_recovery_end)
                    except Exception as exc:
                        finish_span(self._actives_span, error=exc)
                    else:
                        finish_span(self._actives_span)
                    finally:
                        self._actives_span = None

                    # recovery done.
                    self.log.info('Done reading from changelog topics')
                    T(consumer.pause_partitions)(active_tps)
                else:
                    self.log.info('Resuming flow...')
                    T(consumer.resume_flow)()
                    T(self.app.flow_control.resume)()

                self.log.info('Recovery complete')
                if span:
                    span.set_tag('Recovery-Completed', True)
                self.in_recovery = False

                if standby_tps:
                    self.log.info('Starting standby partitions...')

                    self.log.dev('Seek standby offsets')
                    await self._wait(
                        T(self._seek_offsets)(
                            consumer, standby_tps, standby_offsets, 'standby'))

                    self.log.dev('Build standby highwaters')
                    await self._wait(
                        T(self._build_highwaters)(
                            consumer,
                            standby_tps,
                            standby_highwaters,
                            'standby',
                        ),
                    )

                    for tp in standby_tps:
                        if standby_offsets[tp] > standby_highwaters[tp]:
                            raise ConsistencyError(
                                E_PERSISTED_OFFSET.format(
                                    tp,
                                    standby_offsets[tp],
                                    standby_highwaters[tp],
                                ),
                            )

                    if tracer is not None and span:
                        self._standbys_span = tracer.start_span(
                            'recovery-standbys',
                            child_of=span,
                            tags={'Standby-Stats': self.standby_stats()},
                        )
                    self.log.dev('Resume standby partitions')
                    T(consumer.resume_partitions)(standby_tps)

                # Pause all our topic partitions,
                # to make sure we don't fetch any more records from them.
                await self._wait(asyncio.sleep(0.1))  # still needed?
                await self._wait(T(self.on_recovery_completed)())
            except RebalanceAgain as exc:
                self.log.dev('RAISED REBALANCE AGAIN')
                for _span in spans:
                    finish_span(_span, error=exc)
                continue  # another rebalance started
            except ServiceStopped as exc:
                self.log.dev('RAISED SERVICE STOPPED')
                for _span in spans:
                    finish_span(_span, error=exc)
                break  # service was stopped
            except Exception as exc:
                for _span in spans:
                    finish_span(_span, error=exc)
                raise
            else:
                for _span in spans:
                    finish_span(_span)
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
        callback_coros = [
            table.on_recovery_completed(
                self.actives_for_table[table],
                self.standbys_for_table[table],
            )
            for table in self.tables.values()
        ]
        if callback_coros:
            await asyncio.wait(callback_coros)
        assignment = consumer.assignment()
        if assignment:
            self.log.info('Seek stream partitions to committed offsets.')
            await consumer.perform_seek()
        self.completed.set()
        self.log.dev('Resume stream partitions')
        consumer.resume_partitions({
            tp for tp in assignment
            if not self._is_changelog_tp(tp)
        })
        # finally make sure the fetcher is running.
        await self.app._fetcher.maybe_start()
        self.tables.on_actives_ready()
        if not self.app.assignor.assigned_standbys():
            self.tables.on_standbys_ready()
        self.app.on_rebalance_end()
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
            last_value = destination[tp]
            new_value = earliest[tp]

            if last_value is None:
                destination[tp] = new_value
            elif new_value is None:
                destination[tp] = last_value
            else:
                destination[tp] = max(last_value, new_value)
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
        new_offsets = {}
        for tp in tps:
            offset = offsets[tp]
            if offset == -1:
                offset = 0
            new_offsets[tp] = offset
        # FIXME Remove check when fixed offset-1 discrepancy
        await consumer.seek_wait(new_offsets)

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

            seen_offset = offsets.get(tp, None)
            if seen_offset is None or offset > seen_offset:
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
                if self._actives_span is not None:
                    self._actives_span.set_tag('Actives-Ready', True)
                self.signal_recovery_end.set()
            if self.standbys_pending and not self.standby_remaining_total():
                if self._standbys_span:
                    finish_span(self._standbys_span)
                    self._standbys_span = None
                self.tables.on_standbys_ready()

    def flush_buffers(self) -> None:
        for table, buffer in self.buffers.items():
            table.apply_changelog_batch(buffer)
            buffer.clear()

    def need_recovery(self) -> bool:
        return any(v for v in self.active_remaining().values())

    def active_remaining(self) -> Counter[TP]:
        highwaters = self.active_highwaters
        offsets = self.active_offsets
        return Counter({
            tp: highwater - offsets[tp]
            for tp, highwater in highwaters.items()
            if highwater is not None and offsets[tp] is not None
        })

    def standby_remaining(self) -> Counter[TP]:
        highwaters = self.standby_highwaters
        offsets = self.standby_offsets
        return Counter({
            tp: highwater - offsets[tp]
            for tp, highwater in highwaters.items()
            if highwater >= 0 and offsets[tp] >= 0
        })

    def active_remaining_total(self) -> int:
        return sum(self.active_remaining().values())

    def standby_remaining_total(self) -> int:
        return sum(self.standby_remaining().values())

    def active_stats(self) -> MutableMapping[TP, Tuple[int, int, int]]:
        offsets = self.active_offsets
        return {
            tp: (highwater, offsets[tp], highwater - offsets[tp])
            for tp, highwater in self.active_highwaters.items()
            if offsets[tp] is not None and highwater - offsets[tp] != 0
        }

    def standby_stats(self) -> MutableMapping[TP, Tuple[int, int, int]]:
        offsets = self.standby_offsets
        return {
            tp: (highwater, offsets[tp], highwater - offsets[tp])
            for tp, highwater in self.standby_highwaters.items()
            if offsets[tp] is not None and highwater - offsets[tp] != 0

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
