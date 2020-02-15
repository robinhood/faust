"""Table recovery after rebalancing."""
import asyncio
import statistics
import typing

from collections import defaultdict, deque
from time import monotonic
from typing import (
    Any,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Set,
    cast,
)

import opentracing
from mode import Service
from mode.services import WaitArgT
from mode.utils.locks import Event
from mode.utils.times import humanize_seconds, humanize_seconds_ago
from mode.utils.typing import Counter, Deque

from faust.exceptions import ConsistencyError
from faust.types import AppT, EventT, TP
from faust.types.tables import CollectionT, TableManagerT
from faust.types.transports import ConsumerT
from faust.utils import terminal
from faust.utils.terminal.tables import TableDataT  # List[List[str]]
from faust.utils.tracing import finish_span, traced_from_parent_span

if typing.TYPE_CHECKING:
    from faust.app import App as _App
    from .manager import TableManager as _TableManager
else:
    class _App: ...           # noqa
    class _TableManager: ...  # noqa

E_PERSISTED_OFFSET = '''\
The persisted offset for changelog topic partition {0} is higher
than the last offset in that topic (highwater) ({1} > {2}).

Most likely you have removed data from the topics without
removing the RocksDB database file for this partition.
'''


class RecoveryStats(NamedTuple):
    highwater: int
    offset: int
    remaining: int


RecoveryStatsMapping = Mapping[TP, RecoveryStats]


class ServiceStopped(Exception):
    """The recovery service was stopped."""


class RebalanceAgain(Exception):
    """During rebalance, another rebalance happened."""


class Recovery(Service):
    """Service responsible for recovering tables from changelog topics."""

    app: AppT

    tables: _TableManager

    stats_interval: float = 5.0

    #: Set of standby topic partitions.
    standby_tps: Set[TP]

    #: Set of active topic partitions.
    active_tps: Set[TP]

    actives_for_table: MutableMapping[CollectionT, Set[TP]]
    standbys_for_table: MutableMapping[CollectionT, Set[TP]]

    #: Mapping from topic partition to table
    tp_to_table: MutableMapping[TP, CollectionT]

    #: Active offset by topic partition.
    active_offsets: Counter[TP]

    #: Standby offset by topic partition.
    standby_offsets: Counter[TP]

    #: Mapping of highwaters by topic partition.
    highwaters: Counter[TP]

    #: Active highwaters by topic partition.
    active_highwaters: Counter[TP]

    #: Standby highwaters by topic partition.
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

    #: Cache of max buffer size by topic partition..
    buffer_sizes: MutableMapping[TP, int]

    #: Time in seconds after we warn that no flush has happened.
    flush_timeout_secs: float = 120.0

    #: Time in seconds after we warn that no events have been received.
    event_timeout_secs: float = 30.0

    #: Time of last event received by active TP
    _active_events_received_at: MutableMapping[TP, float]

    #: Time of last event received by standby TP
    _standby_events_received_at: MutableMapping[TP, float]

    #: Time of last event received (for any active TP)
    _last_active_event_processed_at: Optional[float]

    #: Time of last buffer flush
    _last_flush_at: Optional[float] = None

    #: Time when recovery last started
    _recovery_started_at: Optional[float] = None

    #: Time when recovery last ended
    _recovery_ended_at: Optional[float] = None

    _recovery_span: Optional[opentracing.Span] = None
    _actives_span: Optional[opentracing.Span] = None
    _standbys_span: Optional[opentracing.Span] = None

    #: List of last 100 processing timestamps (monotonic).
    #: Updated after processing every changelog record,
    #: used to estimate time remaining.
    _processing_times: Deque[float]

    #: Number of entries in _processing_times before
    #: we can give an estimate of time remaining.
    num_samples_required_for_estimate = 1000

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

        self._active_events_received_at = {}
        self._standby_events_received_at = {}
        self._processing_times = deque()

        super().__init__(**kwargs)

    @property
    def signal_recovery_start(self) -> Event:
        """Event used to signal that recovery has started."""
        if self._signal_recovery_start is None:
            self._signal_recovery_start = Event(loop=self.loop)
        return self._signal_recovery_start

    @property
    def signal_recovery_end(self) -> Event:
        """Event used to signal that recovery has ended."""
        if self._signal_recovery_end is None:
            self._signal_recovery_end = Event(loop=self.loop)
        return self._signal_recovery_end

    @property
    def signal_recovery_reset(self) -> Event:
        """Event used to signal that recovery is restarting."""
        if self._signal_recovery_reset is None:
            self._signal_recovery_reset = Event(loop=self.loop)
        return self._signal_recovery_reset

    async def on_stop(self) -> None:
        """Call when recovery service stops."""
        # Flush buffers when stopping.
        self.flush_buffers()

    def add_active(self, table: CollectionT, tp: TP) -> None:
        """Add changelog partition to be used for active recovery."""
        self.active_tps.add(tp)
        self.actives_for_table[table].add(tp)
        self._add(table, tp, self.active_offsets)

    def add_standby(self, table: CollectionT, tp: TP) -> None:
        """Add changelog partition to be used for standby recovery."""
        self.standby_tps.add(tp)
        self.standbys_for_table[table].add(tp)
        self._add(table, tp, self.standby_offsets)

    def _add(self, table: CollectionT, tp: TP, offsets: Counter[TP]) -> None:
        self.tp_to_table[tp] = table
        persisted_offset = table.persisted_offset(tp)
        if persisted_offset is not None:
            offsets[tp] = persisted_offset
        offsets.setdefault(tp, None)  # type: ignore

    def revoke(self, tp: TP) -> None:
        """Revoke assignment of table changelog partition."""
        self.standby_offsets.pop(tp, None)
        self.standby_highwaters.pop(tp, None)
        self.active_offsets.pop(tp, None)
        self.active_highwaters.pop(tp, None)

    def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        """Call when rebalancing and partitions are revoked."""
        T = traced_from_parent_span()
        T(self.flush_buffers)()
        self.signal_recovery_reset.set()

    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        """Call when cluster is rebalancing."""
        app = self.app
        assigned_standbys = app.assignor.assigned_standbys()
        assigned_actives = app.assignor.assigned_actives()

        for tp in revoked:
            await asyncio.sleep(0)
            self.revoke(tp)

        self.standby_tps.clear()
        self.active_tps.clear()
        self.actives_for_table.clear()
        self.standbys_for_table.clear()

        for tp in assigned_standbys:
            table = self.tables._changelogs.get(tp.topic)
            if table is not None:
                self.add_standby(table, tp)
            await asyncio.sleep(0)
        await asyncio.sleep(0)
        for tp in assigned_actives:
            table = self.tables._changelogs.get(tp.topic)
            if table is not None:
                self.add_active(table, tp)
            await asyncio.sleep(0)
        await asyncio.sleep(0)

        active_offsets = {
            tp: offset
            for tp, offset in self.active_offsets.items()
            if tp in self.active_tps
        }
        self.active_offsets.clear()
        self.active_offsets.update(active_offsets)

        await asyncio.sleep(0)

        rebalancing_span = cast(_App, self.app)._rebalancing_span
        if app.tracer and rebalancing_span:
            self._recovery_span = app.tracer.get_tracer('_faust').start_span(
                'recovery',
                child_of=rebalancing_span,
            )
            app._span_add_default_tags(self._recovery_span)
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
        await cast(_App, app)._fetcher.maybe_start()
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
            self._set_recovery_ended()
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
                self.app._span_add_default_tags(span)
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

                self._set_recovery_started()
                self.standbys_pending = True
                # Must flush any buffers before starting rebalance.
                T(self.flush_buffers)()
                producer = cast(_App, self.app)._producer
                if producer is not None:
                    await self._wait(T(producer.flush)())

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
                    await T(cast(_App, self.app)._fetcher.maybe_start)()
                    T(self.app.flow_control.resume)()

                    # Wait for actives to be up to date.
                    # This signal will be set by _slurp_changelogs
                    if tracer is not None and span:
                        self._actives_span = tracer.start_span(
                            'recovery-actives',
                            child_of=span,
                            tags={'Active-Stats': self.active_stats()},
                        )
                        self.app._span_add_default_tags(span)
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
                self._set_recovery_ended()

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
                        self.app._span_add_default_tags(span)
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
        self._set_recovery_ended()

    def _set_recovery_started(self) -> None:
        self.in_recovery = True
        self._recovery_ended = None
        self._recovery_started_at = monotonic()
        self._active_events_received_at.clear()
        self._standby_events_received_at.clear()
        self._processing_times.clear()
        self._last_active_event_processed_at = None

    def _set_recovery_ended(self) -> None:
        self.in_recovery = False
        self._recovery_ended_at = monotonic()
        self._active_events_received_at.clear()
        self._standby_events_received_at.clear()
        self._processing_times.clear()
        self._last_active_event_processed_at = None

    def active_remaining_seconds(self, remaining: float) -> str:
        s = self._estimated_active_remaining_secs(remaining)
        return humanize_seconds(s, now='none') if s else '???'

    def _estimated_active_remaining_secs(
            self, remaining: float) -> Optional[float]:
        processing_times = self._processing_times
        if len(processing_times) >= self.num_samples_required_for_estimate:
            mean_time = statistics.mean(processing_times)
            return (mean_time * remaining) * 1.10  # add 10%
        else:
            return None

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
        """Call when active table recovery is completed."""
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
        await cast(_App, self.app)._fetcher.maybe_start()
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
            tp: value - 1 if value is not None else -1
            for tp, value in highwaters.items()
        }
        self.log.info(
            'Highwater for %s changelog partitions:\n%s',
            title, self._highwater_logtable(highwaters, title=title))
        destination.clear()
        destination.update(highwaters)

    def _highwater_logtable(self, highwaters: Mapping[TP, int], *,
                            title: str) -> str:
        table_data = [
            [k.topic, str(k.partition), str(v)]
            for k, v in sorted(highwaters.items())
        ]
        return terminal.logtable(
            list(self._consolidate_table_keys(table_data)),
            title=f'Highwater - {title.capitalize()}',
            headers=['topic', 'partition', 'highwater'],
        )

    def _consolidate_table_keys(self, data: TableDataT) -> Iterator[List[str]]:
        """Format terminal log table to reduce noise from duplicate keys.

        We log tables where the first row is the name of the topic,
        and it gets noisy when that name is repeated over and over.

        This function replaces repeating topic names
        with the ditto mark.

        Note:
            Data must be sorted.
        """
        prev_key: Optional[str] = None
        for key, *rest in data:
            if prev_key is not None and prev_key == key:
                yield ['〃', *rest]  # ditto
            else:
                yield [key, *rest]
            prev_key = key

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
        self.log.info(
            '%s offsets at start of reading:\n%s',
            title,
            self._start_offsets_logtable(destination, title=title),
        )

    def _start_offsets_logtable(self, offsets: Mapping[TP, int], *,
                                title: str) -> str:
        table_data = [
            [k.topic, str(k.partition), str(v)]
            for k, v in sorted(offsets.items())
        ]
        return terminal.logtable(
            list(self._consolidate_table_keys(table_data)),
            title=f'Reading Starts At - {title.capitalize()}',
            headers=['topic', 'partition', 'offset'],
        )

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
        active_events_received_at = self._active_events_received_at
        standby_events_received_at = self._standby_events_received_at

        buffers = self.buffers
        buffer_sizes = self.buffer_sizes
        processing_times = self._processing_times

        def _maybe_signal_recovery_end() -> None:
            if self.in_recovery and not self.active_remaining_total():
                # apply anything stuck in the buffers
                self.flush_buffers()
                self._set_recovery_ended()
                if self._actives_span is not None:
                    self._actives_span.set_tag('Actives-Ready', True)
                self.signal_recovery_end.set()

        while not self.should_stop:
            try:
                event: EventT = await asyncio.wait_for(
                    changelog_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                if self.should_stop:
                    return
                _maybe_signal_recovery_end()
                continue

            now = monotonic()
            message = event.message
            tp = message.tp
            offset = message.offset

            offsets: Counter[TP]
            bufsize = buffer_sizes.get(tp)
            is_active = False
            if tp in active_tps:
                is_active = True
                table = tp_to_table[tp]
                offsets = active_offsets
                if bufsize is None:
                    bufsize = buffer_sizes[tp] = table.recovery_buffer_size
                active_events_received_at[tp] = now
            elif tp in standby_tps:
                table = tp_to_table[tp]
                offsets = standby_offsets
                if bufsize is None:
                    bufsize = buffer_sizes[tp] = table.standby_buffer_size
                    standby_events_received_at[tp] = now
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
                    self._last_flush_at = now
                now_after = monotonic()

                if is_active:
                    last_processed_at = self._last_active_event_processed_at
                    if last_processed_at is not None:
                        processing_times.append(now_after - last_processed_at)
                        max_samples = self.num_samples_required_for_estimate
                        if len(processing_times) > max_samples:
                            processing_times.popleft()
                    self._last_active_event_processed_at = now_after

            _maybe_signal_recovery_end()

            if self.standbys_pending and not self.standby_remaining_total():
                if self._standbys_span:
                    finish_span(self._standbys_span)
                    self._standbys_span = None
                self.tables.on_standbys_ready()

    def flush_buffers(self) -> None:
        """Flush changelog buffers."""
        for table, buffer in self.buffers.items():
            table.apply_changelog_batch(buffer)
            buffer.clear()
        self._last_flush_at = monotonic()

    def need_recovery(self) -> bool:
        """Return :const:`True` if recovery is required."""
        return any(v for v in self.active_remaining().values())

    def active_remaining(self) -> Counter[TP]:
        """Return counter of remaining changes by active partition."""
        highwaters = self.active_highwaters
        offsets = self.active_offsets
        return Counter({
            tp: highwater - offsets[tp]
            for tp, highwater in highwaters.items()
            if highwater is not None and offsets[tp] is not None
        })

    def standby_remaining(self) -> Counter[TP]:
        """Return counter of remaining changes by standby partition."""
        highwaters = self.standby_highwaters
        offsets = self.standby_offsets
        return Counter({
            tp: highwater - offsets[tp]
            for tp, highwater in highwaters.items()
            if highwater >= 0 and offsets[tp] >= 0
        })

    def active_remaining_total(self) -> int:
        """Return number of changes remaining for actives to be up-to-date."""
        return sum(self.active_remaining().values())

    def standby_remaining_total(self) -> int:
        """Return number of changes remaining for standbys to be up-to-date."""
        return sum(self.standby_remaining().values())

    def active_stats(self) -> RecoveryStatsMapping:
        """Return current active recovery statistics."""
        offsets = self.active_offsets
        return {
            tp: RecoveryStats(highwater,
                              offsets[tp],
                              highwater - offsets[tp])
            for tp, highwater in self.active_highwaters.items()
            if offsets[tp] is not None and highwater - offsets[tp] != 0
        }

    def standby_stats(self) -> RecoveryStatsMapping:
        """Return current standby recovery statistics."""
        offsets = self.standby_offsets
        return {
            tp: RecoveryStats(highwater,
                              offsets[tp],
                              highwater - offsets[tp])
            for tp, highwater in self.standby_highwaters.items()
            if offsets[tp] is not None and highwater - offsets[tp] != 0

        }

    def _stats_to_logtable(
            self,
            title: str,
            stats: RecoveryStatsMapping) -> str:
        table_data = [
            list(map(str, [
                tp.topic,
                tp.partition,
                s.highwater,
                s.offset,
                s.remaining,
            ])) for tp, s in sorted(stats.items())
        ]
        return terminal.logtable(
            list(self._consolidate_table_keys(table_data)),
            title=title,
            headers=[
                'topic',
                'partition',
                'need offset',
                'have offset',
                'remaining',
            ],
        )

    @Service.task
    async def _publish_stats(self) -> None:
        """Emit stats (remaining to fetch) while in active recovery."""
        interval = self.stats_interval
        await self.sleep(interval)
        async for sleep_time in self.itertimer(
                interval, name='Recovery.stats'):
            if self.in_recovery:
                now = monotonic()
                stats = self.active_stats()
                num_samples = len(self._processing_times)
                if stats and \
                        num_samples >= self.num_samples_required_for_estimate:
                    remaining_total = self.active_remaining_total()
                    self.log.info(
                        'Still fetching changelog topics for recovery, '
                        'estimated time remaining %s '
                        '(total remaining=%r):\n%s',
                        self.active_remaining_seconds(remaining_total),
                        remaining_total,
                        self._stats_to_logtable(
                            'Remaining for active recovery', stats),
                    )
                elif stats:
                    await self._verify_remaining(now, stats)
                else:
                    recovery_started_at = self._recovery_started_at
                    if recovery_started_at is None:
                        self.log.error(
                            'POSSIBLE INTERNAL ERROR: '
                            'Recovery marked as started but missing '
                            'self._recovery_started_at timestamp.')
                    else:
                        secs_since_started = now - recovery_started_at
                        if secs_since_started >= 30.0:
                            # This shouldn't happen, but we want to
                            # log an error in case it does.
                            self.log.error(
                                'POSSIBLE INTERNAL ERROR: '
                                'Recovery has no remaining offsets to fetch, '
                                'but we have spent %s waiting for the worker '
                                'to transition out of recovery state...',
                                humanize_seconds(secs_since_started),
                            )

    async def _verify_remaining(
            self,
            now: float,
            stats: RecoveryStatsMapping) -> None:
        consumer = self.app.consumer
        active_events_received_at = self._active_events_received_at
        recovery_started_at = self._recovery_started_at
        if recovery_started_at is None:
            return  # we already log about this in _publish_stats
        secs_since_started = now - recovery_started_at

        last_flush_at = self._last_flush_at
        if last_flush_at is None:
            if secs_since_started >= self.flush_timeout_secs:
                self.log.warning(
                    'Recovery has not flushed buffers since '
                    'recovery startted (started %s). '
                    'Current total buffer size: %r',
                    humanize_seconds_ago(secs_since_started),
                    self._current_total_buffer_size(),
                )
        else:
            secs_since_last_flush = now - last_flush_at
            if secs_since_last_flush >= self.flush_timeout_secs:
                self.log.warning(
                    'Recovery has not flushed buffers in the last %r '
                    'seconds (last flush was %s). '
                    'Current total buffer size: %r',
                    self.flush_timeout_secs,
                    humanize_seconds_ago(secs_since_last_flush),
                    self._current_total_buffer_size(),
                )

        for tp in stats:
            await self.sleep(0)
            if self.should_stop:
                break
            if not self.in_recovery:
                break
            consumer.verify_recovery_event_path(now, tp)
            secs_since_started = now - recovery_started_at

            last_event_received = active_events_received_at.get(tp)
            if last_event_received is None:
                if secs_since_started >= self.event_timeout_secs:
                    self.log.warning(
                        'No event received for active tp %r since recovery '
                        'start (started %s)',
                        tp, humanize_seconds_ago(secs_since_started),
                    )
                continue

            secs_since_received = now - last_event_received
            if secs_since_received >= self.event_timeout_secs:
                self.log.warning(
                    'No event received for active tp %r in the last %r '
                    'seconds (last event received %s)',
                    tp, self.event_timeout_secs,
                    humanize_seconds_ago(secs_since_received),
                )

    def _current_total_buffer_size(self) -> int:
        return sum(len(buf) for buf in self.buffers.values())

    def _is_changelog_tp(self, tp: TP) -> bool:
        return tp.topic in self.tables.changelog_topics
