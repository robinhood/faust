import asyncio
import pytest

from mode import label, shortlabel
from mode.utils.futures import done_future
from mode.utils.mocks import AsyncMock, Mock, patch

from faust import App, Channel, Topic
from faust.transport.consumer import Consumer
from faust.transport.conductor import Conductor
from faust.types import Message, TP

TP1 = TP('foo', 0)
TP2 = TP('foo', 1)


class test_Conductor:

    @pytest.fixture
    def con(self, *, app):
        return Conductor(app)

    @pytest.fixture
    def con_client_only(self, *, app):
        app.client_only = True
        return Conductor(app)

    def test_constructor(self, *, con):
        assert con._topics == set()
        assert con._topic_name_index == {}
        assert con._tp_index == {}
        assert con._tp_to_callback == {}
        assert con._acking_topics == set()
        assert con._subscription_changed is None
        assert con._subscription_done is None
        assert con._compiler
        assert con.on_message

    def test_acks_enabled_for(self, *, con):
        assert not con.acks_enabled_for('foo')
        con._acking_topics.add('foo')
        assert con.acks_enabled_for('foo')

    @pytest.mark.asyncio
    async def test_con_client_only(self, *, con_client_only):
        assert con_client_only.on_message
        message = Mock(name='message')
        tp = TP(topic=message.topic, partition=0)
        cb = con_client_only._tp_to_callback[tp] = AsyncMock(name='cb')

        ret = await con_client_only.on_message(message)
        assert ret is cb.coro.return_value

        cb.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_commit(self, *, con):
        con.app = Mock(
            name='app',
            autospec=App,
            consumer=Mock(
                autospec=Consumer,
                commit=AsyncMock(),
            ),
        )
        await con.commit({TP1})

        con.app.consumer.commit.assert_called_once_with({TP1})

    @pytest.mark.asyncio
    async def test_on_message(self, *, con):
        cb = con._tp_to_callback[TP1] = AsyncMock(name='callback')
        message = Mock(name='message', autospec=Message)
        message.tp = TP1
        await con.on_message(message)
        cb.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_on_message_decoded_only_once(self, *, con):
        """A message is decoded only once per (key_type, value_type)."""
        def _chan(name, key_type, value_type):
            chan = AsyncMock(name=name, autospec=Channel)
            chan.key_type = key_type
            chan.value_type = value_type
            # needed to test chan.queue.put_nowait method
            chan.queue.full = lambda: False
            return chan

        # those methods are not useful for the purpose of the test
        # and they block it if not mocked.
        con.app.producer.buffer.wait_until_ebb = AsyncMock()
        con.app.flow_control.acquire = AsyncMock()
        con.app.sensors.on_topic_buffer_full = AsyncMock()

        chan1 = _chan('1', 1, 1)
        chan2 = _chan('2', 1, 1)
        chan3 = _chan('3', 2, 2)
        chan4 = _chan('4', 2, 2)
        message = Mock(name='message', autospec=Message)

        # make sure the chan iteration is ordered, by using a list,
        # to make later asserts relevant
        cb = con._build_handler(TP1, [chan1, chan2, chan3, chan4])

        await cb(message)

        # ensure that chans reusing an already decoded message does not
        # try to decode it and make sure they are sending it to their queue.
        chan1.decode.assert_called_once()
        chan2.decode.assert_not_called()
        event1 = chan1.queue.put_nowait.call_args[0][0]
        chan2.queue.put_nowait.assert_called_once_with(event1)

        chan3.decode.assert_called_once()
        chan4.decode.assert_not_called()
        event3 = chan3.queue.put_nowait.call_args[0][0]
        chan4.queue.put_nowait.assert_called_once_with(event3)

    @pytest.mark.asyncio
    async def test_wait_for_subscriptions(self, *, con):
        with patch('asyncio.Future', AsyncMock()) as Future:
            con._subscription_done = None
            await con.wait_for_subscriptions()
            Future.assert_called_once_with(loop=con.loop)

    @pytest.mark.asyncio
    async def test_wait_for_subscriptions__done(self, *, con):
        con._subscription_done = done_future()
        await con.wait_for_subscriptions()

    @pytest.mark.asyncio
    async def test_update_indices(self, *, con):
        topic1 = Mock(name='topic1', autospec=Topic)
        topic1.acks = False
        topic1.topics = ['t1']
        topic1.internal = False
        topic2 = Mock(name='topic2', autospec=Topic)
        topic2.acks = True
        topic2.topics = ['t2']
        topic2.internal = True
        topic2.maybe_declare = AsyncMock(name='maybe_declare')
        con._topics = {topic1, topic2}

        await con._update_indices()
        topic1.maybe_declare.assert_not_called()
        topic2.maybe_declare.assert_called_once_with()
        assert 't1' not in con._acking_topics
        assert 't2' in con._acking_topics
        assert con._topic_name_index['t1'] == {topic1}
        assert con._topic_name_index['t2'] == {topic2}

    @pytest.mark.asyncio
    async def test_on_partitions_assigned(self, *, con):
        con._tp_index = {1: 2}
        con._update_tp_index = Mock(name='_update_tp_index')
        con._update_callback_map = Mock(name='_update_callback_map')
        assigned = {TP1, TP2}
        await con.on_partitions_assigned(assigned)
        assert not con._tp_index
        con._update_tp_index.assert_called_once_with(assigned)
        con._update_callback_map.assert_called_once_with()

    def test_update_tp_index(self, *, con):
        assigned = {TP1, TP2}
        topic1 = Mock(name='topic1', autospec=Topic)
        topic1.topics = [TP1.topic]
        topic1.active_partitions = None
        con._topics.add(topic1)
        con._update_tp_index(assigned)
        assert topic1 in con._tp_index[TP1]
        assert topic1 in con._tp_index[TP2]

    def test_update_tp_index__active_partitions(self, *, con):
        assigned = {TP1, TP2}
        topic1 = Mock(name='topic1', autospec=Topic)
        topic1.active_partitions = {TP1, TP2}
        con._topics.add(topic1)
        con._update_tp_index(assigned)
        assert topic1 in con._tp_index[TP1]
        assert topic1 in con._tp_index[TP2]
        con._update_tp_index(set())

    def test_update_tp_index__active_partitions_empty(self, *, con):
        assigned = {TP1, TP2}
        topic1 = Mock(name='topic1', autospec=Topic)
        topic1.active_partitions = set()
        con._topics.add(topic1)
        con._update_tp_index(assigned)
        assert topic1 not in con._tp_index[TP1]
        assert topic1 not in con._tp_index[TP2]

    def test_update_callback_map(self, *, con):
        chan1 = Mock(name='chan1', autospec=Channel)
        chan2 = Mock(name='chan2', autospec=Channel)
        con._tp_index = {
            TP1: chan1,
            TP2: chan2,
        }
        con._update_callback_map()

        assert con._tp_to_callback[TP1]
        assert con._tp_to_callback[TP2]

    def test_clear(self, *, con):
        con._topics = {'t1'}
        con._topic_name_index = {2: 3}
        con._tp_index = {3: 4}
        con._tp_to_callback = {4: 5}
        con._acking_topics = {1, 2, 3}
        con.clear()

        assert not con._topics
        assert not con._topic_name_index
        assert not con._tp_index
        assert not con._tp_to_callback
        assert not con._acking_topics

    def test_iter(self, *, con):
        con._topics = {'1', '2'}
        assert sorted(iter(con)) == ['1', '2']

    def test_hash(self, *, con):
        assert hash(con)

    def test_add(self, *, app, con):
        con._topics = set()
        con._topic_name_index = {}
        topic = app.topic('foo')
        con.add(topic)
        assert topic in con._topics
        con.add(topic)
        con.add(app.topic('bar'))

    def test_topic_contain_unsubcribed_topics(self, *, app, con):
        con._topic_name_index = {'foo': {}}
        assert con._topic_contain_unsubscribed_topics(app.topic('bar'))
        assert not con._topic_contain_unsubscribed_topics(app.topic('foo'))

    def test_flag_changes(self, *, con):
        con._subscription_changed = None
        con._subscription_done = None
        con._flag_changes()
        assert con._subscription_done is not None

        con._subscription_done = None
        con._subscription_changed = asyncio.Event()
        con._flag_changes()
        assert con._subscription_changed.is_set()
        assert con._subscription_done is not None
        con._flag_changes()

    def test_add_flags_changes(self, *, con, app):
        topic = app.topic('foo', 'bar')
        con._flag_changes = Mock(name='flag_changes')
        con._topic_name_index['baz'].add(topic)
        con.add(topic)
        con._flag_changes.assert_called_once_with()

    def test_discard(self, *, con, app):
        topic = app.topic('foo', 'bar')
        con.add(topic)
        assert topic in con._topics
        con.discard(topic)
        assert topic not in con._topics

    def test_label(self, *, con):
        assert label(con)

    @pytest.mark.asyncio
    async def test_wait_for_subscriptions__notset(self, *, con):
        with patch('asyncio.Future') as Future:
            Future.return_value = done_future()
            await con.wait_for_subscriptions()

    @pytest.mark.asyncio
    async def test_maybe_wait_for_subscriptions(self, *, con):
        con._subscription_done = done_future()
        await con.maybe_wait_for_subscriptions()

    @pytest.mark.asyncio
    async def test_maybe_wait_for_subscriptions__none(self, *, con):
        con._subscription_done = None
        await con.maybe_wait_for_subscriptions()

    @pytest.mark.asyncio
    async def test_on_client_only_start(self, *, con, app):
        topic = app.topic('foo', 'bar')
        con.add(topic)
        await con.on_client_only_start()
        assert con._tp_index[TP(topic='foo', partition=0)] == {topic}
        assert con._tp_index[TP(topic='bar', partition=0)] == {topic}

    def test_shortlabel(self, *, con):
        assert shortlabel(con)
