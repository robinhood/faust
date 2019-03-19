import asyncio
import operator
from copy import copy

import pytest
from faust import joins
from faust import Event, Record, Stream, Topic
from faust.exceptions import PartitionsMismatch
from faust.stores.base import Store
from faust.tables.base import Collection
from faust.types import TP
from faust.windows import Window
from mode import label, shortlabel
from mode.utils.mocks import AsyncMock, Mock, patch

TP1 = TP('foo', 0)


class User(Record):
    id: str
    name: str


class MyTable(Collection):

    def __post_init__(self, *args, **kwargs):
        self.datas = {}

    def _has_key(self, key):
        return key in self.datas

    def _get_key(self, key):
        return self.datas.get(key)

    def _set_key(self, key, value):
        self.datas[key] = value

    def _del_key(self, key):
        self.datas.pop(key, None)


class test_Collection:

    @pytest.fixture
    def table(self, *, app):
        return MyTable(app, name='name')

    def test_key_type_bytes_implies_raw_serializer(self, *, app):
        table = MyTable(app, name='name', key_type=bytes)
        assert table.key_serializer == 'raw'

    @pytest.mark.asyncio
    async def test_init_on_recover(self, *, app):
        on_recover = AsyncMock(name='on_recover')
        t = MyTable(app, name='name', on_recover=on_recover)
        assert on_recover in t._recover_callbacks
        await t.call_recover_callbacks()
        on_recover.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_recovery_completed(self, *, table):
        table.call_recover_callbacks = AsyncMock()
        await table.on_recovery_completed(set(), set())
        table.call_recover_callbacks.assert_called_once_with()

    def test_hash(self, *, table):
        assert hash(table)

    @pytest.mark.asyncio
    async def test_on_start(self, *, table):
        table.changelog_topic = Mock(
            name='changelog_topic',
            autospec=Topic,
            maybe_declare=AsyncMock(),
        )
        await table.on_start()
        table.changelog_topic.maybe_declare.assert_called_once_with()

    def test_info(self, *, table):
        assert table.info() == {
            'app': table.app,
            'name': table.name,
            'store': table._store,
            'default': table.default,
            'key_type': table.key_type,
            'value_type': table.value_type,
            'changelog_topic': table._changelog_topic,
            'window': table.window,
        }

    def test_persisted_offset(self, *, table):
        data = table._data = Mock(name='_data')
        assert table.persisted_offset(TP1) == data.persisted_offset()

    @pytest.mark.asyncio
    async def test_need_active_standby_for(self, *, table):
        table._data = Mock(
            name='_data',
            autospec=Store,
            need_active_standby_for=AsyncMock(),
        )
        assert (await table.need_active_standby_for(TP1) ==
                table._data.need_active_standby_for.coro())

    def test_reset_state(self, *, table):
        data = table._data = Mock(name='_data', autospec=Store)
        table.reset_state()
        data.reset_state.assert_called_once_with()

    def test_send_changelog(self, *, table):
        event = Mock(name='event')
        table._send_changelog(event, 'k', 'v')
        event._attach.assert_called_once_with(
            table.changelog_topic,
            'k',
            'v',
            partition=event.message.partition,
            key_serializer='json',
            value_serializer='json',
            callback=table._on_changelog_sent,
        )

    def test_send_changelog__custom_serializers(self, *, table):
        event = Mock(name='event')
        table._send_changelog(
            event, 'k', 'v',
            key_serializer='raw',
            value_serializer='raw',
        )
        event._attach.assert_called_once_with(
            table.changelog_topic,
            'k',
            'v',
            partition=event.message.partition,
            key_serializer='raw',
            value_serializer='raw',
            callback=table._on_changelog_sent,
        )

    def test_send_changelog__no_current_event(self, *, table):
        with pytest.raises(RuntimeError):
            table._send_changelog(None, 'k', 'v')

    def test_on_changelog_sent(self, *, table):
        fut = Mock(name='future', autospec=asyncio.Future)
        table._data = Mock(name='data', autospec=Store)
        table._on_changelog_sent(fut)
        table._data.set_persisted_offset.assert_called_once_with(
            fut.result().topic_partition, fut.result().offset,
        )

    def test_on_changelog_sent__transactions(self, *, table):
        table.app.in_transaction = True
        table.app.tables = Mock(name='tables')
        fut = Mock(name='fut')
        table._on_changelog_sent(fut)
        table.app.tables.persist_offset_on_commit.assert_called_once_with(
            table.data, fut.result().topic_partition, fut.result().offset,
        )

    def test_del_old_keys__empty(self, *, table):
        table.window = Mock(name='window')
        table._del_old_keys()

    def test_del_old_keys(self, *, table):
        table.window = Mock(name='window')
        table._data = {
            'boo': 'BOO',
            'moo': 'MOO',
            'faa': 'FAA',
        }
        table._partition_timestamps = {
            TP1: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        }
        table._partition_timestamp_keys = {
            (TP1, 2.0): ['boo', 'moo', 'faa'],
        }

        def is_stale(timestamp, latest_timestamp):
            return timestamp < 4.0

        table.window.stale.side_effect = is_stale

        table._del_old_keys()

        assert table._partition_timestamps[TP1] == [4.0, 5.0, 6.0, 7.0]
        assert not table.data

    @pytest.mark.parametrize('source_n,change_n,expect_error', [
        (3, 3, False),
        (3, None, False),
        (None, 3, False),
        (3, 6, True),
        (6, 3, True),
    ])
    def test__verify_source_topic_partitions(
            self, source_n, change_n, expect_error, *, app, table):
        event = Mock(name='event', autospec=Event)
        tps = {
            event.message.topic: source_n,
            table.changelog_topic.get_topic_name(): change_n,
        }
        app.consumer.topic_partitions = Mock(side_effect=tps.get)
        if expect_error:
            with pytest.raises(PartitionsMismatch):
                table._verify_source_topic_partitions(event)
        else:
            table._verify_source_topic_partitions(event)

    @pytest.mark.asyncio
    async def test_clean_data(self, *, table):
        table._should_expire_keys = Mock(name='_should_expire_keys')
        table._should_expire_keys.return_value = False
        await table._clean_data(table)

        table._should_expire_keys.return_value = True
        table._del_old_keys = Mock(name='_del_old_keys')

        def on_sleep(secs):
            table._stopped.set()
        table.sleep = AsyncMock(name='sleep', side_effect=on_sleep)

        await table._clean_data(table)

        table._del_old_keys.assert_called_once_with()
        table.sleep.assert_called_once_with(
            table.app.conf.table_cleanup_interval)

    def test_should_expire_keys(self, *, table):
        table.window = None
        assert not table._should_expire_keys()
        table.window = Mock(name='window', autospec=Window)
        table.window.expires = 3600
        assert table._should_expire_keys()

    def test_join(self, *, table):
        table._join = Mock(name='join')
        ret = table.join(User.id, User.name)
        table._join.assert_called_once_with(
            joins.RightJoin(stream=table, fields=(User.id, User.name)),
        )
        assert ret is table._join()

    def test_left_join(self, *, table):
        table._join = Mock(name='join')
        ret = table.left_join(User.id, User.name)
        table._join.assert_called_once_with(
            joins.LeftJoin(stream=table, fields=(User.id, User.name)),
        )
        assert ret is table._join()

    def test_inner_join(self, *, table):
        table._join = Mock(name='join')
        ret = table.inner_join(User.id, User.name)
        table._join.assert_called_once_with(
            joins.InnerJoin(stream=table, fields=(User.id, User.name)),
        )
        assert ret is table._join()

    def test_outer_join(self, *, table):
        table._join = Mock(name='join')
        ret = table.outer_join(User.id, User.name)
        table._join.assert_called_once_with(
            joins.OuterJoin(stream=table, fields=(User.id, User.name)),
        )
        assert ret is table._join()

    def test__join(self, *, table):
        with pytest.raises(NotImplementedError):
            table._join(Mock(name='join_strategy', autospec=joins.Join))

    def test_clone(self, *, table):
        t2 = table.clone()
        assert t2.info() == table.info()

    def test_combine(self, *, table):
        with pytest.raises(NotImplementedError):
            table.combine(Mock(name='joinable', autospec=Stream))

    def test_contribute_to_stream(self, *, table):
        table.contribute_to_stream(Mock(name='stream', autospec=Stream))

    @pytest.mark.asyncio
    async def test_remove_from_stream(self, *, table):
        await table.remove_from_stream(Mock(name='stream', autospec=Stream))

    def test_new_changelog_topic__window_expires(self, *, table):
        table.window = Mock(name='window', autospec=Window)
        table.window.expires = 3600.3
        assert table._new_changelog_topic(retention=None).retention == 3600.3

    def test_new_changelog_topic__default_compacting(self, *, table):
        table._changelog_compacting = True
        assert table._new_changelog_topic(compacting=None).compacting
        table._changelog_compacting = False
        assert not table._new_changelog_topic(compacting=None).compacting
        assert table._new_changelog_topic(compacting=True).compacting

    def test_new_changelog_topic__default_deleting(self, *, table):
        table._changelog_deleting = True
        assert table._new_changelog_topic(deleting=None).deleting
        table._changelog_deleting = False
        assert not table._new_changelog_topic(deleting=None).deleting
        assert table._new_changelog_topic(deleting=True).deleting

    def test_copy(self, *, table):
        assert copy(table).info() == table.info()

    def test_and(self, *, table):
        with pytest.raises(NotImplementedError):
            table & table

    def test__maybe_set_key_ttl(self, *, table):
        table._should_expire_keys = Mock(return_value=False)
        table._maybe_set_key_ttl(('k', (100, 110)), 0)

        table._should_expire_keys = Mock(return_value=True)
        table._maybe_set_key_ttl(('k', (100, 110)), 0)

    def test__maybe_del_key_ttl(self, *, table):
        table._partition_timestamp_keys[(0, 110)] = None

        table._should_expire_keys = Mock(return_value=False)
        table._maybe_del_key_ttl(('k', (100, 110)), 0)

        table._should_expire_keys = Mock(return_value=True)
        table._maybe_del_key_ttl(('k', (100, 110)), 0)

        table._partition_timestamp_keys[(0, 110)] = {
            ('k', (100, 110)),
            ('v', (100, 110)),
        }
        table._maybe_del_key_ttl(('k', (100, 110)), 0)

        assert table._partition_timestamp_keys[(0, 110)] == {('v', (100, 110))}

    def test_apply_window_op(self, *, table):
        self.mock_ranges(table)
        table._set_key(('k', 1.1), 30)
        table._set_key(('k', 1.2), 40)
        table._set_key(('k', 1.3), 50)
        table._apply_window_op(operator.add, 'k', 12, 300.3)

        assert table._get_key(('k', 1.1)) == 42
        assert table._get_key(('k', 1.2)) == 52
        assert table._get_key(('k', 1.3)) == 62

    def test_set_del_windowed(self, *, table):
        ranges = self.mock_ranges(table)
        table._set_windowed('k', 11, 300.3)
        for r in ranges:
            assert table._get_key(('k', r)) == 11
        table._del_windowed('k', 300.3)
        for r in ranges:
            assert table._get_key(('k', r)) is None

    def test_window_ranges(self, *, table):
        table.window = Mock(name='window', autospec=Window)
        table.window.ranges.return_value = [1, 2, 3]
        assert list(table._window_ranges(300.3)) == [1, 2, 3]

    def mock_ranges(self, table, ranges=[1.1, 1.2, 1.3]):  # noqa
        table._window_ranges = Mock(name='_window_ranges')
        table._window_ranges.return_value = ranges
        return ranges

    def test_relative_now(self, *, table):
        event = Mock(name='event', autospec=Event)
        table._partition_latest_timestamp[event.message.partition] = 30.3
        assert table._relative_now(event) == 30.3

    def test_relative_now__no_event(self, *, table):
        with patch('faust.tables.base.current_event') as ce:
            ce.return_value = None
            with patch('time.time') as time:
                assert table._relative_now(None) is time()

    def test_relative_event(self, *, table):
        event = Mock(name='event', autospec=Event)
        assert table._relative_event(event) is event.message.timestamp

    def test_relative_event__raises_if_no_event(self, *, table):
        with patch('faust.tables.base.current_event') as current_event:
            current_event.return_value = None
            with pytest.raises(RuntimeError):
                table._relative_event(None)

    def test_relative_field(self, *, table):
        user = User('foo', 'bar')
        event = Mock(name='event', autospec=Event)
        event.value = user
        assert table._relative_field(User.id)(event) == 'foo'

    def test_relative_field__raises_if_no_event(self, *, table):
        with pytest.raises(RuntimeError):
            table._relative_field(User.id)(event=None)

    def test_relative_timestamp(self, *, table):
        assert table._relative_timestamp(303.3)(
            Mock(name='event', autospec=Event)) == 303.3

    def test_windowed_now(self, *, table):
        with patch('faust.tables.base.current_event'):
            table.window = Mock(name='window', autospec=Window)
            table.window.earliest.return_value = 42
            table._get_key = Mock(name='_get_key')
            table._windowed_now('k')
            table._get_key.assert_called_once_with(('k', 42))

    def test_windowed_timestamp(self, *, table):
        table.window = Mock(name='window', autospec=Window)
        table.window.current.return_value = 10.1
        assert not table._windowed_contains('k', 303.3)
        table._set_key(('k', 10.1), 101.1)
        assert table._windowed_timestamp('k', 303.3) == 101.1
        assert table._windowed_contains('k', 303.3)

    def test_windowed_delta(self, *, table):
        event = Mock(name='event', autospec=Event)
        table.window = Mock(name='window', autospec=Window)
        table.window.delta.return_value = 10.1
        table._set_key(('k', 10.1), 101.1)
        assert table._windowed_delta('k', 303.3, event=event) == 101.1

    @pytest.mark.asyncio
    async def test_on_rebalance(self, *, table):
        table._data = Mock(
            name='data',
            autospec=Store,
            on_rebalance=AsyncMock(),
        )
        await table.on_rebalance({TP1}, set(), set())
        table._data.on_rebalance.assert_called_once_with(
            table, {TP1}, set(), set())

    @pytest.mark.asyncio
    async def test_on_changelog_event(self, *, table):
        event = Mock(name='event', autospec=Event)
        table._on_changelog_event = None
        await table.on_changelog_event(event)
        table._on_changelog_event = AsyncMock(name='callback')
        await table.on_changelog_event(event)
        table._on_changelog_event.assert_called_once_with(event)

    def test_label(self, *, table):
        assert label(table)

    def test_shortlabel(self, *, table):
        assert shortlabel(table)

    def test_apply_changelog_batch(self, *, table):
        table._data = Mock(name='data', autospec=Store)
        table.apply_changelog_batch([1, 2, 3])
        table._data.apply_changelog_batch.assert_called_once_with(
            [1, 2, 3],
            to_key=table._to_key,
            to_value=table._to_value,
        )

    def test_to_key(self, *, table):
        assert table._to_key([1, 2, 3]) == (1, 2, 3)
        assert table._to_key(1) == 1

    def test_to_value(self, *, table):
        v = Mock(name='v')
        assert table._to_value(v) is v

    def test__human_channel(self, *, table):
        assert table._human_channel()

    def test_repr_info(self, *, table):
        assert table._repr_info() == table.name
