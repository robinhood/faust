from collections import Counter
import pytest
from faust.tables.recovery import RebalanceAgain, Recovery, ServiceStopped
from faust.types import TP
from mode.utils.mocks import AsyncMock, Mock

TP1 = TP('foo', 6)
TP2 = TP('bar', 3)
TP3 = TP('baz', 1)
TP4 = TP('xuz', 0)


@pytest.fixture()
def tables():
    return Mock(name='tables')


@pytest.fixture()
def recovery(*, tables, app):
    return Recovery(app, tables)


class test_Recovery:

    @pytest.fixture()
    def table(self):
        return Mock(name='table')

    def test_init(self, *, recovery, tables):
        assert recovery.tables is tables
        assert recovery.signal_recovery_start
        assert recovery.signal_recovery_end
        assert recovery.signal_recovery_reset

    @pytest.mark.asyncio
    async def test_on_stop(self, *, recovery):
        recovery.flush_buffers = Mock()
        await recovery.on_stop()
        recovery.flush_buffers.assert_called_once_with()

    def test_add_active(self, *, recovery, table):
        recovery.add_active(table, TP1)
        assert TP1 in recovery.active_tps
        assert TP1 in recovery.actives_for_table[table]
        assert recovery.tp_to_table[TP1] is table
        assert recovery.active_offsets[TP1] is table.persisted_offset()
        recovery.revoke(TP1)
        assert TP1 not in recovery.active_offsets

    def test_add_standby(self, *, recovery, table):
        recovery.add_standby(table, TP1)
        assert TP1 in recovery.standby_tps
        assert TP1 in recovery.standbys_for_table[table]
        assert recovery.tp_to_table[TP1] is table
        assert recovery.standby_offsets[TP1] is table.persisted_offset()
        recovery.revoke(TP1)
        assert TP1 not in recovery.standby_offsets

    def test_on_partitions_revoked(self, *, recovery):
        recovery.signal_recovery_reset.clear()
        recovery.flush_buffers = Mock()

        recovery.on_partitions_revoked({TP1})

        recovery.flush_buffers.assert_called_once_with()
        assert recovery.signal_recovery_reset.is_set()

    @pytest.mark.asyncio
    async def test_on_rebalance(self, *, recovery, app, tables):
        recovery.signal_recovery_reset.set()
        app.assignor = Mock()
        app.assignor.assigned_standbys.return_value = {TP1}
        app.assignor.assigned_actives.return_value = {TP2}
        tables._changelogs = {
            TP1.topic: Mock(name='table1'),
            TP2.topic: Mock(name='table2'),
        }
        await recovery.on_rebalance({TP1, TP2, TP3}, {TP4}, {TP3})
        assert recovery.signal_recovery_start.is_set()
        assert not recovery.signal_recovery_reset.is_set()

        assert TP1 in recovery.standby_tps
        assert TP2 in recovery.active_tps

    @pytest.mark.asyncio
    async def test_on_rebalance__empty(self, *, recovery, app):
        app.assignor = Mock()
        app.assignor.assigned_standbys.return_value = set()
        app.assignor.assigned_actives.return_value = set()
        await recovery.on_rebalance(set(), set(), set())
        assert recovery.signal_recovery_start.is_set()

    @pytest.mark.asyncio
    async def test__resume_streams(self, *, recovery, tables, app):
        app.tables = tables
        app.on_rebalance_complete = Mock(send=AsyncMock())
        app.on_rebalance_end = Mock()
        app.flow_control = Mock()
        app._fetcher = Mock(maybe_start=AsyncMock())
        consumer = app.consumer = Mock()
        recovery._wait = AsyncMock()

        await recovery._resume_streams()
        app.on_rebalance_complete.send.assert_called_once_with()
        consumer.resume_flow.assert_called_once_with()
        app.flow_control.resume.assert_called_once_with()
        recovery._wait.assert_called_once_with(consumer.perform_seek())
        consumer.resume_partitions.assert_called_once_with(
            consumer.assignment())

        assert recovery.completed.is_set()
        app._fetcher.maybe_start.assert_called_once_with()
        app.tables.on_actives_ready.assert_called_once_with()
        app.tables.on_standbys_ready.assert_called_once_with()
        app.on_rebalance_end.assert_called_once_with()

        consumer.assignment.return_value = set()
        await recovery._resume_streams()

    @pytest.mark.asyncio
    async def test__wait(self, *, recovery):
        assert await self.assert_wait(
            recovery, stopped=False, done=None) is None

    @pytest.mark.asyncio
    async def test__wait__stopped(self, *, recovery):
        with pytest.raises(ServiceStopped):
            await self.assert_wait(recovery, stopped=True, done=None)

    @pytest.mark.asyncio
    async def test__wait__recovery_restart(self, *, recovery):
        with pytest.raises(RebalanceAgain):
            await self.assert_wait(
                recovery,
                stopped=False,
                done=recovery.signal_recovery_start)

    @pytest.mark.asyncio
    async def test__wait__recovery_reset(self, *, recovery):
        with pytest.raises(RebalanceAgain):
            await self.assert_wait(
                recovery,
                stopped=False,
                done=recovery.signal_recovery_reset)

    async def assert_wait(self, recovery,
                          stopped=False,
                          done=None):
        coro = Mock()
        recovery.wait_first = AsyncMock()
        recovery.wait_first.coro.return_value.stopped = stopped
        recovery.wait_first.coro.return_value.done = {done} if done else set()

        ret = await recovery._wait(coro)
        recovery.wait_first.assert_called_once_with(
            coro,
            recovery.signal_recovery_reset,
            recovery.signal_recovery_start,
        )
        return ret

    @pytest.mark.asyncio
    async def test_on_recovery_completed(self, *, recovery, tables, app):
        assignment = {TP1, TP2, TP3, TP4}
        consumer = app.consumer = Mock(
            name='consumer',
            perform_seek=AsyncMock(),
            assignment=Mock(return_value=assignment),
        )
        app.assignor = Mock(assigned_standbys=Mock(return_value={TP2}))
        recovery._is_changelog_tp = Mock(return_value=True)
        app._fetcher = Mock(maybe_start=AsyncMock())
        app.on_rebalance_complete = Mock(send=AsyncMock())
        app.on_rebalance_end = Mock()
        table1 = Mock(name='table1', on_recovery_completed=AsyncMock())
        table2 = Mock(name='table2', on_recovery_completed=AsyncMock())
        tables.values.return_value = [table1, table2]
        recovery.actives_for_table[table1] = {TP1}
        recovery.actives_for_table[table2] = {TP2}
        recovery.standbys_for_table[table1] = {TP3}
        recovery.standbys_for_table[table2] = {TP4}

        await recovery.on_recovery_completed()
        app.on_rebalance_complete.send.assert_called_once_with()
        table1.on_recovery_completed.assert_called_once_with({TP1}, {TP3})
        table2.on_recovery_completed.assert_called_once_with({TP2}, {TP4})
        consumer.perform_seek.assert_called_once_with()
        assert recovery.completed.is_set()
        consumer.resume_partitions.assert_called_once()
        app._fetcher.maybe_start.assert_called_once_with()
        tables.on_actives_ready.assert_called_once_with()
        app.on_rebalance_end.assert_called_once_with()

        assignment.clear()
        app.assignor.assigned_standbys.return_value = set()
        await recovery.on_recovery_completed()

        tables.values.return_value = []
        await recovery.on_recovery_completed()

    @pytest.mark.asyncio
    async def test__build_highwaters(self, *, recovery):
        tps = {TP1, TP2, TP3}
        dest = Counter({TP1: 103, TP4: 10})
        consumer = Mock(
            name='consumer',
            highwaters=AsyncMock(
                return_value={
                    TP1: 1001,
                    TP2: 0,
                    TP3: 202,
                },
            ),
        )
        await recovery._build_highwaters(consumer, tps, dest, 'title')
        assert dest == Counter({
            TP1: 1000,
            TP2: -1,
            TP3: 201,
        })

    @pytest.mark.asyncio
    async def test__build_offsets(self, *, recovery):
        tps = {TP1, TP2, TP3}
        dest = Counter({
            TP1: 300,
            TP2: 101,
            TP3: 2003,
        })
        consumer = Mock(
            name='consumer',
            earliest_offsets=AsyncMock(
                return_value={
                    TP1: 0,
                    TP2: 201,
                    TP3: 3003,
                },
            ),
        )
        await recovery._build_offsets(consumer, tps, dest, 'title')
        assert dest == Counter({
            TP1: 300,
            TP2: 200,
            TP3: 3002,
        })

    @pytest.mark.asyncio
    async def test__seek_offsets(self, *, recovery):
        consumer = Mock(
            name='consumer',
            seek_wait=AsyncMock(),
        )
        offsets = {
            TP1: -1,
            TP2: 1001,
            TP3: 2002,
        }
        tps = {TP1, TP2, TP3}

        await recovery._seek_offsets(consumer, tps, offsets, 'seek')
        consumer.seek_wait.assert_called_once_with({
            TP1: 0,
            TP2: 1001,
            TP3: 2002,
        })

    def test_flush_buffers(self, *, recovery):
        recovery.buffers.update({
            Mock(name='table1'): Mock(name='buffer1'),
            Mock(name='table2'): Mock(name='buffer2'),
        })
        recovery.flush_buffers()

        assert len(recovery.buffers) == 2
        for table, buffer in recovery.buffers.items():
            table.apply_changelog_batch.assert_called_once_with(buffer)
            buffer.clear.assert_called_once_with()

    def test_need_recovery__yes(self, *, recovery):
        self._setup_active_offsets(recovery)
        assert recovery.need_recovery()

    def test_need_recovery__no(self, *, recovery):
        self._setup_active_offsets(recovery)
        recovery.active_offsets = recovery.active_highwaters
        assert not recovery.need_recovery()

    def test_active_remaining(self, *, recovery):
        self._setup_active_offsets(recovery)
        assert recovery.active_remaining_total() == 2002

    def test_standby_remaining(self, *, recovery):
        self._setup_standby_offsets(recovery)
        assert recovery.standby_remaining_total() == 2002

    def test_active_stats(self, *, recovery):
        self._setup_active_offsets(recovery)
        assert recovery.active_stats() == {
            TP2: (3003, 2002, 1001),
            TP3: (4004, 3003, 1001),
        }

    def _setup_active_offsets(self, recovery):
        recovery.active_offsets = Counter({
            TP1: 1001,
            TP2: 2002,
            TP3: 3003,
        })
        recovery.active_highwaters = Counter({
            TP1: 1001,
            TP2: 3003,
            TP3: 4004,
        })

    def test_standby_stats(self, *, recovery):
        self._setup_standby_offsets(recovery)
        assert recovery.standby_stats() == {
            TP2: (3003, 2002, 1001),
            TP3: (4004, 3003, 1001),
        }

    def _setup_standby_offsets(self, recovery):
        recovery.standby_offsets = Counter({
            TP1: 1001,
            TP2: 2002,
            TP3: 3003,
        })
        recovery.standby_highwaters = Counter({
            TP1: 1001,
            TP2: 3003,
            TP3: 4004,
        })

    def test__is_changelog_tp(self, *, recovery, tables):
        tables.changelog_topics = {TP1.topic}
        assert recovery._is_changelog_tp(TP1)


@pytest.mark.parametrize('highwaters,offsets,needs_recovery,total,remaining', [
    ({TP1: 0, TP2: -1}, {TP1: -1, TP2: -1}, True, 1, {TP1: 1, TP2: 0}),
    ({TP1: -1, TP2: -1}, {TP1: -1, TP2: -1}, False, 0, {TP1: 0, TP2: 0}),
    ({TP1: 100, TP2: -1}, {TP1: -1, TP2: -1}, True, 101, {TP1: 101, TP2: 0}),
])
def test_recovery_from_offset_0(
        highwaters, offsets, needs_recovery, total, remaining, *,
        recovery):
    recovery.active_highwaters.update(highwaters)
    recovery.active_offsets.update(offsets)

    if needs_recovery:
        assert recovery.need_recovery()
    else:
        assert not recovery.need_recovery()
    assert recovery.active_remaining_total() == total
    if remaining:
        assert recovery.active_remaining() == remaining
