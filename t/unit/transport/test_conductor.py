import asyncio
import pytest
from faust import App, Channel, Topic
from faust.transport.consumer import Consumer
from faust.transport.conductor import Conductor
from faust.types import Message, TP
from mode import label, shortlabel
from mode.utils.futures import done_future
from mode.utils.mocks import AsyncMock, Mock

TP1 = TP('foo', 0)
TP2 = TP('foo', 1)


class test_Conductor:

    @pytest.fixture
    def con(self, *, app):
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
    async def test_wait_for_subscription(self, *, con):
        con._subscription_done = None
        await con.wait_for_subscriptions()
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

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, con):
        con._tp_index = {1: 2}
        await con.on_partitions_revoked(set())
        assert not con._tp_index

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
        assert sorted(list(iter(con))) == ['1', '2']

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

    def test_label(self, *, con):
        assert label(con)

    def test_shortlabel(self, *, con):
        assert shortlabel(con)
