from unittest.mock import Mock
import pytest
from faust.tables.changelogs import ChangelogReader, StandbyReader, local_tps
from faust.types import TP
from mode import label
from mode.utils.futures import done_future

TP1 = TP('foo', 0)
TP2 = TP('foo', 1)
TPS = {TP1, TP2}


@pytest.fixture
def table(*, app):
    return app.Table('name')


@pytest.fixture
def channel():
    return Mock(name='channel')


class TypeEq:

    def __init__(self, typ):
        self.typ = typ

    def __eq__(self, other):
        return type(other) == self.typ


class test_ChangelogReader:
    Reader = ChangelogReader

    @pytest.fixture
    def reader(self, *, app, channel, table):
        return self.Reader(table, channel, app, TPS)

    def test_constructor(self, *, app, channel, table, reader):
        assert reader.table is table
        assert reader.channel is channel
        assert reader.app is app
        assert reader.tps is TPS

    def test_buffer_size(self, *, table, reader):
        assert reader._buffer_size == table.recovery_buffer_size

    @pytest.mark.asyncio
    async def test_on_stop(self, *, channel, reader):
        channel.throw.return_value = done_future()
        await reader.on_stop()
        channel.throw.assert_called_once_with(TypeEq(StopAsyncIteration))
        reader._stop_event.set()
        await reader.on_stop()

    @pytest.mark.asyncio
    async def test_build_highwaters(self, *, app, reader):
        app.consumer = Mock(name='consumer')
        highwaters = {
            TP1: 3003,
            TP2: 6006,
        }
        reader._highwaters = {'foo': 'moo'}
        app.consumer.highwaters.return_value = done_future(highwaters)
        await reader._build_highwaters()
        assert reader._highwaters == {
            TP1: 3002,
            TP2: 6005,
        }

    def set_highwaters(self, reader, tp, highwater, offset):
        reader._highwaters[tp] = highwater
        reader.offsets[tp] = offset

    def test_should_stop_reading(self, *, reader):
        reader.offsets.clear()
        self.set_highwaters(reader, TP1, 3003, 3003)
        assert reader._should_stop_reading()

    def test_should_stop_reading__not_done(self, *, reader):
        reader.offsets.clear()
        self.set_highwaters(reader, TP1, 3003, 3002)
        assert not reader._should_stop_reading()

    def test_remaining(self, *, reader):
        reader.offsets.clear()
        self.set_highwaters(reader, TP1, 3003, 2003)
        assert reader._remaining() == {TP1: 1000}

    def test_remaining_total(self, *, reader):
        reader.offsets.clear()
        self.set_highwaters(reader, TP1, 3003, 2003)
        self.set_highwaters(reader, TP2, 1001, 1)
        assert reader._remaining_total() == 2000

    @pytest.mark.asyncio
    async def test_update_offsets(self, *, app, reader):
        app.consumer = Mock(name='consumer')
        earliest = {
            TP1: 30,
            TP2: 0,
        }
        self.set_highwaters(reader, TP1, 1000, 31)
        self.set_highwaters(reader, TP2, 1001, 0)
        app.consumer.earliest_offsets.return_value = done_future(earliest)
        await reader._update_offsets()
        assert reader.offsets == {TP1: 31, TP2: 0}

    @pytest.mark.asyncio
    async def test_seek_tps(self, *, app, reader):
        app.consumer = Mock(name='consumer')
        self.set_highwaters(reader, TP1, 3003, 2003)
        self.set_highwaters(reader, TP2, 1001, 1)
        app.consumer.seek.return_value = done_future()

        def on_position(tp):
            return done_future(reader.offsets[tp])
        app.consumer.position.side_effect = on_position

        await reader._seek_tps()

    def test_should_start_reading(self, *, reader):
        self.set_highwaters(reader, TP1, 3003, 2003)
        self.set_highwaters(reader, TP2, 1001, 1)
        assert reader._should_start_reading()

    @pytest.mark.asyncio
    async def test_wait_done_reading(self, *, reader):
        reader._stop_event.set()
        await reader.wait_done_reading()

    def test_done_reading(self, *, reader):
        reader._done_reading()
        assert reader._stop_event.is_set()
        assert reader._shutdown.is_set()

    def test_remaining_stats(self, *, reader):
        self.set_highwaters(reader, TP1, 3003, 2003)
        self.set_highwaters(reader, TP2, 1001, 1)
        assert reader._remaining_stats == {
            TP1: (3003, 2003, 1000),
            TP2: (1001, 1, 1000),
        }

    def test_recovered(self, *, reader):
        self.set_highwaters(reader, TP1, 3003, 2003)
        self.set_highwaters(reader, TP2, 1001, 1)
        assert not reader.recovered()
        self.set_highwaters(reader, TP1, 3003, 3003)
        self.set_highwaters(reader, TP2, 1001, 1001)
        assert reader.recovered()

    @pytest.mark.asyncio
    async def test_publish_stats(self, *, reader):
        def on_sleep(secs):
            reader._stopped.set()
            return done_future()
        reader.sleep = Mock(name='sleep')
        reader.sleep.side_effect = on_sleep
        await reader._publish_stats(reader)

    def test_label(self, *, reader):
        assert label(reader)

    @pytest.mark.asyncio
    async def test_local_tps(self, *, table):
        table.need_active_standby_for = Mock(name='need_active_standby_for')

        def need_standby(tp):
            if tp == TP1:
                return done_future(True)
            return done_future(False)
        table.need_active_standby_for.side_effect = need_standby

        assert await local_tps(table, {TP1, TP2}) == {TP2}


class test_StandbyReader(test_ChangelogReader):
    Reader = StandbyReader

    def test_buffer_size(self, *, reader):
        assert reader._buffer_size == reader.table.standby_buffer_size

    @pytest.mark.asyncio
    async def test_publish_stats(self, *, reader):
        await reader._publish_stats(reader)

    def test_should_start_reading(self, *, reader):
        assert reader._should_start_reading()

    def test_should_stop_reading(self, *, reader):
        assert not reader._should_stop_reading()
        reader._stopped.set()
        assert reader._should_stop_reading()

    def test_recovered(self, *, reader):
        assert not reader.recovered()
