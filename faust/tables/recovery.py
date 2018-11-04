import typing
from collections import defaultdict
from typing import Any, List, MutableMapping, Set, cast

from mode import Service
from mode.utils.compat import Counter

from faust.types import AppT, EventT, TP
from faust.types.tables import CollectionT, TableManagerT
from faust.types.transports import ConsumerT
from faust.utils import terminal

if typing.TYPE_CHECKING:
    from .manager import TableManager
else:
    class TableManager: ...  # noqa


class Recovery(Service):

    app: AppT

    tables: TableManager

    #: Set of standby tps.
    standby_tps: Set[TP]

    #: Set of active tps.
    active_tps: Set[TP]

    #: Set of all table tps.
    tps: Set[TP]

    #: Mapping of tps by table.
    tps_by_table: MutableMapping[CollectionT, Set[TP]]

    #: Mapping from TP to table
    tp_to_table: MutableMapping[TP, CollectionT]

    #: Mapping of offsets by tp.
    offsets: MutableMapping[TP, int]

    #: Mapping of highwaters by tp.
    highwaters: MutableMapping[TP, int]

    def __init__(self,
                 app: AppT,
                 tables: TableManagerT,
                 **kwargs: Any) -> None:
        self.app = app
        self.tables = cast(TableManager, tables)
        self.standby_tps = set()
        self.active_tps = set()
        self.tps = set()
        self.tps_by_table = defaultdict(set)
        self.tp_to_table = {}
        self.offsets = Counter()
        self.highwaters = {}
        super().__init__(**kwargs)

    def add_active(self, tp: TP) -> None:
        table = self.tables._changelogs[tp.topic]
        self.log.info('Adding active for table %s: %s', table, tp)
        self.active_tps.add(tp)
        self._add(table, tp)

    def remove_active(self, tp: TP) -> None:
        table = self.tables._changelogs[tp.topic]
        self.log.info('Removing active for table %s: %s', table, tp)
        self.active_tps.discard(tp)
        self._remove(table, tp)

    def add_standby(self, tp: TP) -> None:
        table = self.tables._changelogs[tp.topic]
        self.log.info('Adding standby for table %s: %s', table, tp)
        self.standby_tps.add(tp)
        self._add(table, tp)

    def remove_standby(self, tp: TP) -> None:
        table = self.tables._changelogs[tp.topic]
        self.log.info('Removing standby for table %s: %s', table, tp)
        self.standby_tps.discard(tp)
        self._remove(table, tp)

    def _add(self, table: CollectionT, tp: TP) -> None:
        self.tps_by_table[table].add(tp)
        self.tp_to_table[tp] = table
        self.tps.add(tp)
        persisted_offset = table.persisted_offset(tp)
        if persisted_offset is not None:
            curr_offset = self.offsets.get(tp, -1)
            self.offsets[tp] = max(curr_offset, persisted_offset)
        self.offsets.setdefault(tp, -1)

    def _remove(self, table: CollectionT, tp: TP) -> None:
        self.tps_by_table[table].discard(tp)
        self.tp_to_table.pop(tp, None)
        self.tps.discard(tp)
        self.offsets.pop(tp, None)
        self.highwaters.pop(tp, None)

    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        self.log.info('Updating actives/standbys after rebalance...')
        consumer = self.app.consumer
        tps = self.tps
        offsets = self.offsets
        standby_tps = self.app.assignor.assigned_standbys()
        # for table in self.values():
        #     standby_tps = await local_tps(table, standby_tps)
        assigned_tps = self.app.assignor.assigned_actives()

        for tp in revoked:
            if tp in self.active_tps:
                self.remove_active(tp)
            elif tp in self.standby_tps:
                self.remove_standby(tp)

        for tp in newly_assigned:
            table = self.tables._changelogs.get(tp.topic)
            if table is not None:
                if await table.need_active_standby_for(tp):
                    if tp in standby_tps:
                        self.add_standby(tp)
                    elif tp in assigned_tps:
                        self.add_active(tp)
                    else:
                        pass   # belongs to agent?

        print('ACTIVE TPS: %r' % (self.active_tps,))
        print('STANDBY TPS: %r' % (self.standby_tps,))
        print('ALL TPS: %r' % (self.tps,))

        # Pause all table changelog partitions
        await consumer.pause_partitions(tps)
        await self._build_highwaters(consumer)
        await self._build_offsets(consumer)

        # Seek to new offsets
        for tp in tps:
            offset = offsets[tp]
            if offset >= 0:
                # FIXME Remove check when fixed offset-1 discrepancy
                await consumer.seek(tp, offset)
                assert await consumer.position(tp) == offset

        self.log.info('Restoring state from changelog topics...')

        # Resume partitions and start fetching.
        await consumer.resume_partitions(tps)
        await self.app._fetcher.start()

        await self.wait_for_actives()

        self.log.info('Done reading from changelog topics')
        # Pause all our topic partitions,
        # to make sure we don't fetch any more records from them.
        assignment = self.app.consumer.assignment()
        await consumer.pause_partitions(
            {tp for tp in tps if tp in assignment})
        await self.tables.on_recovery_completed()

    async def wait_for_actives(self):
        print('SHOULD WAIT FOR ACTIVES')

    @Service.task
    async def _slurp_changelogs(self):
        changelog_queue = self.tables.changelog_queue
        tp_to_table = self.tp_to_table
        offsets = self.offsets

        buffers: MutableMapping[CollectionT, List[EventT]] = defaultdict(list)

        buffer_sizes: MutableMapping[TP, int] = {}

        while not self.should_stop:
            event: EventT = await changelog_queue.get()
            message = event.message
            tp = message.tp
            offset = message.offset
            seen_offset = offsets.get(tp, -1)
            if offset > seen_offset:
                offsets[tp] = offset

                table = tp_to_table[tp]
                bufsize = buffer_sizes.get(tp)
                if bufsize is None:
                    if tp in self.active_tps:
                        bufsize = buffer_sizes[tp] = table.active_buffer_size
                    else:
                        bufsize = buffer_sizes[tp] = table.standby_buffer_size
                buf = buffers[table]
                buf.append(event)
                await table.on_changelog_event(event)
                if len(buf) >= bufsize:
                    print('APPLYING BUF')
                    table.apply_changelog_batch(buf)
                    buf.clear()

    async def _build_highwaters(self, consumer: ConsumerT):
        # -- Build highwater
        highwaters = await consumer.highwaters(*self.tps)
        highwaters = {
            # FIXME the -1 here is because of the way we commit offsets
            tp: value - 1
            for tp, value in highwaters.items()
        }
        table = terminal.logtable(
            [[k.topic, str(k.partition), str(v)]
             for k, v in highwaters.items()],
            title='Highwater',
            headers=['topic', 'partition', 'highwater'],
        )
        self.log.info('Highwater for changelog partitions:\n%s', table)
        self.highwaters.clear()
        self.highwaters.update(highwaters)

    async def _build_offsets(self, consumer: ConsumerT):
        # -- Update offsets
        # Offsets may have been compacted, need to get to the recent ones
        earliest = await consumer.earliest_offsets(*self.tps)
        # FIXME To be consistent with the offset -1 logic
        earliest = {tp: offset - 1 for tp, offset in earliest.items()}
        offsets = self.offsets
        for tp in self.tps:
            offsets[tp] = max(offsets[tp], earliest[tp])
        table = terminal.logtable(
            [(k.topic, k.partition, v) for k, v in offsets.items()],
            title='Reading Starts At',
            headers=['topic', 'partition', 'offset'],
        )
        self.log.info('Updated offsets at start of reading:\n%s', table)
        offsets.clear()
        offsets.update(offsets)
