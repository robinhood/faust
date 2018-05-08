import asyncio
import operator
from copy import copy

import pytest
from faust import joins
from faust import Event, Record, Stream, Topic
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

    def __init__(self, *args, **kwargs):
        self.datas = {}
        super().__init__(*args, **kwargs)

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

    @pytest.mark.asyncio
    async def test_init_on_recover(self, *, app):
        on_recover = AsyncMock(name='on_recover')
        t = MyTable(app, name='name', on_recover=on_recover)
        assert on_recover in t._recover_callbacks
        await t.call_recover_callbacks()
        on_recover.assert_called_once_with()

    def test_hash(self, *, table):
        assert hash(table)

    def test_get_store_custom_StateStore(self, *, table):
        table.StateStore = Mock(name='StateStore', autospec=Store)
        table._data = None
        ret = table._get_store()
        table.StateStore.assert_called_once_with(
            url=None, app=table.app, loop=table.loop,
        )
        assert ret is table.StateStore()

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
        with patch('faust.tables.base.current_event') as current_event:
            table._send_changelog('k', 'v')
            current_event.return_value._attach.assert_called_once_with(
                table.changelog_topic,
                'k',
                'v',
                partition=current_event.return_value.message.partition,
                key_serializer='json',
                value_serializer='json',
                callback=table._on_changelog_sent,
            )

    def test_send_changelog__no_current_event(self, *, table):
        with patch('faust.tables.base.current_event') as current_event:
            current_event.return_value = None
            with pytest.raises(RuntimeError):
                table._send_changelog('k', 'v')

    def test_on_changelog_sent(self, *, table):
        fut = Mock(name='future', autospec=asyncio.Future)
        table._data = Mock(name='data', autospec=Store)
        table._on_changelog_sent(fut)
        table._data.set_persisted_offset.assert_called_once_with(
            fut.result().topic_partition, fut.result().offset,
        )

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

    def test_relative_event(self, *, table):
        event = Mock(name='event', autospec=Event)
        assert table._relative_event(event) is event.message.timestamp

    def test_relative_field(self, *, table):
        user = User('foo', 'bar')
        event = Mock(name='event', autospec=Event)
        event.value = user
        assert table._relative_field(User.id)(event) == 'foo'

    def test_relative_timestamp(self, *, table):
        assert table._relative_timestamp(303.3)(
            Mock(name='event', autospec=Event)) == 303.3

    def test_windowed_now(self, *, table):
        with patch('faust.tables.base.current_event'):
            table._windowed_timestamp = Mock(name='windowed_timestamp')
            ret = table._windowed_now('k')
            table._windowed_timestamp.assert_called_once_with('k', 0)
            assert ret is table._windowed_timestamp()

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
    async def test_on_partitions_assigned(self, *, table):
        table._data = Mock(
            name='data',
            autospec=Store,
            on_partitions_assigned=AsyncMock(),
        )
        await table.on_partitions_assigned({TP1})
        table._data.on_partitions_assigned.assert_called_once_with(
            table, {TP1})

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, table):
        table._data = Mock(
            name='data',
            autospec=Store,
            on_partitions_revoked=AsyncMock(),
        )
        await table.on_partitions_revoked({TP1})
        table._data.on_partitions_revoked.assert_called_once_with(table, {TP1})

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
