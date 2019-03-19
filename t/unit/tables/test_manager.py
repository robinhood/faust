import pytest
from faust.tables.recovery import Recovery
from faust.types import TP
from mode.utils.mocks import AsyncMock, Mock

TP1 = TP('foo', 3)
TP2 = TP('bar', 4)
TP3 = TP('baz', 5)


class test_Manager:

    @pytest.fixture()
    def tables(self, *, app):
        return app.tables

    def test_persist_offset_on_commit(self, *, tables):
        store = Mock(name='store')
        tables.persist_offset_on_commit(store, TP1, 30)
        assert tables._pending_persisted_offsets[TP1] == (store, 30)

        tables.persist_offset_on_commit(store, TP1, 29)
        assert tables._pending_persisted_offsets[TP1] == (store, 30)

        tables.persist_offset_on_commit(store, TP1, 31)
        assert tables._pending_persisted_offsets[TP1] == (store, 31)

    def test_on_commit(self, *, tables):
        tables.on_commit_tp = Mock(name='on_commit_tp')
        tables.on_commit({TP1: 30})
        tables.on_commit_tp.assert_called_once_with(TP1)

    def test_on_commit_tp(self, *, tables):
        store = Mock(name='store')
        tables.on_commit_tp(TP1)
        tables.persist_offset_on_commit(store, TP1, 30)
        tables.on_commit_tp(TP1)
        store.set_persisted_offset.assert_called_once_with(TP1, 30)

    def test_on_rebalance_start(self, *, tables):
        tables.on_rebalance_start()
        assert not tables.actives_ready
        assert not tables.standbys_ready

        tables.on_actives_ready()
        assert tables.actives_ready

        tables.on_standbys_ready()
        assert tables.standbys_ready

    def test_hash(self, *, tables):
        assert hash(tables)

    def test_changelog_topics(self, *, tables):
        assert tables.changelog_topics == set()

    def test_changelog_queue(self, *, tables, app):
        assert tables.changelog_queue
        assert tables.changelog_queue.maxsize == app.conf.stream_buffer_maxsize

    def test_recovery(self, *, tables):
        assert tables.recovery
        assert isinstance(tables.recovery, Recovery)
        assert tables.recovery.beacon.parent is tables.beacon
        assert tables.recovery.loop is tables.loop

    def test_add(self, *, tables):
        table = Mock(name='table')
        assert tables.add(table) is table
        assert tables[table.name] is table
        assert tables._changelogs[table.changelog_topic.get_topic_name()]

        with pytest.raises(ValueError):
            tables.add(table)  # already exists

    def test_add__when_recovery_started_raises(self, *, tables):
        tables._recovery_started.set()
        with pytest.raises(RuntimeError):
            tables.add(Mock(name='table'))

    @pytest.mark.asyncio
    async def test_on_start(self, *, tables):
        tables.sleep = AsyncMock()
        tables._update_channels = AsyncMock()
        tables._recovery = Mock(start=AsyncMock())

        tables._stopped.set()
        await tables.on_start()

        tables.sleep.assert_called_once_with(1.0)
        tables._update_channels.assert_not_called()
        tables._recovery.start.assert_not_called()

        tables._stopped.clear()
        await tables.on_start()

        tables._update_channels.assert_called_once_with()
        tables._recovery.start.assert_called_once_with()

    @pytest.mark.asyncio
    async def test__update_channels(self, *, tables, app):
        app.consumer = Mock(name='consumer')
        app.consumer.assignment.return_value = set()
        await tables._update_channels()
        app.topics = Mock(name='topics')
        table1 = Mock(name='table', maybe_start=AsyncMock())
        tables.add(table1)
        await tables._update_channels()

        table1.maybe_start.assert_called_once_with()
        assert tables._channels[table1]
        await tables._update_channels()

    @pytest.mark.asyncio
    async def test_on_stop(self, *, tables, app):
        app._fetcher = Mock(name='fetcher', stop=AsyncMock())
        await tables.on_stop()

        app._fetcher.stop.assert_called_once_with()

        table1 = Mock(name='table1', stop=AsyncMock())
        tables._recovery = Mock(name='_recovery', stop=AsyncMock())
        tables.add(table1)
        await tables.on_stop()

        tables._recovery.stop.assert_called_once_with()
        table1.stop.assert_called_once_with()

    def test_on_partitions_revoked(self, *, tables):
        tables._recovery = Mock(name='_recovery')
        tables.on_partitions_revoked({TP1})
        tables._recovery.on_partitions_revoked.assert_called_once_with({TP1})

    @pytest.mark.asyncio
    async def test_on_rebalance(self, *, tables):
        tables._update_channels = AsyncMock()
        tables._recovery = Mock(on_rebalance=AsyncMock())

        await tables.on_rebalance({TP1, TP2, TP3}, set(), {TP1, TP2, TP3})
        tables._recovery_started.clear()

        tables._update_channels.assert_called_once_with()
        tables._recovery.on_rebalance.assert_called_once_with(
            {TP1, TP2, TP3}, set(), {TP1, TP2, TP3})

        table1 = Mock(name='table', on_rebalance=AsyncMock())
        tables.add(table1)

        await tables.on_rebalance({TP2, TP3}, {TP1}, set())
        table1.on_rebalance.assert_called_once_with({TP2, TP3}, {TP1}, set())
