from unittest.mock import Mock
import pytest
from faust.transport.consumer import Fetcher, Consumer
from faust.types import TP
from mode.utils.futures import done_future

TP1 = TP('foo', 0)


class test_Fetcher:

    @pytest.fixture
    def fetcher(self, *, app):
        return Fetcher(app)

    @pytest.mark.asyncio
    async def test_fetcher(self, *, fetcher, app):
        app.consumer = Mock(name='consumer')
        app.consumer._drain_messages.return_value = done_future()

        await fetcher._fetcher(fetcher)

        app.consumer._drain_messages.assert_called_once_with(fetcher)



class MyConsumer(Consumer):

    def assignment(self):
        ...

    async def create_topic(self, *args, **kwargs):
        ...

    def earliest_offsets(self, *args, **kwargs):
        ...

    async def getmany(self, *args, **kwargs):
        ...

    def highwater(self, *args, **kwargs):
        ...

    def highwaters(self, *args, **kwargs):
        ...

    async def pause_partitions(self, *args, **kwargs):
        ...

    async def perform_seek(self, *args, **kwargs):
        ...

    def position(self, *args, **kwargs):
        ...

    async def resume_partitions(self, *args, **kwargs):
        ...

    async def seek(self, *args, **kwargs):
        ...

    async def subscribe(self, *args, **kwargs):
        ...

    async def _commit(self, offsets) -> bool:
        ...

    def _new_topicpartition(self, topic, partition) -> TP:
        return TP(topic, partition)


class test_Consumer:

    @pytest.fixture
    def callback(self):
        return Mock(name='callback')

    @pytest.fixture
    def on_P_revoked(self):
        return Mock(name='on_partitions_revoked')

    @pytest.fixture
    def on_P_assigned(self):
        return Mock(name='on_partitions_assigned')

    @pytest.fixture
    def consumer(self, *,
                 app,
                 callback,
                 on_P_revoked,
                 on_P_assigned):
        return MyConsumer(
            app.transport,
            callback=callback,
            on_partitions_revoked=on_P_revoked,
            on_partitions_assigned=on_P_assigned,
        )

    @pytest.fixture
    def message(self):
        return Mock(name='message')

    def test_is_changelog_tp(self, *, app, consumer):
        app.tables = Mock(name='tables')
        app.tables.changelog_topics = {'foo', 'bar'}
        assert consumer._is_changelog_tp(TP('foo', 31))
        assert consumer._is_changelog_tp(TP('bar', 0))
        assert not consumer._is_changelog_tp(TP('baz', 3))

    @pytest.mark.asyncio
    async def test_on_partitions_assigned(self, *, consumer):
        consumer._on_partitions_assigned = Mock(name='opa')
        consumer._on_partitions_assigned.return_value = done_future()
        tps = {TP('foo', 0), TP('bar', 2)}
        await consumer.on_partitions_assigned(tps)

        consumer._on_partitions_assigned.assert_called_once_with(
            tps)

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, consumer):
        consumer._on_partitions_revoked = Mock(name='opr')
        consumer._on_partitions_revoked.return_value = done_future()
        tps = {TP('foo', 0), TP('bar', 2)}
        await consumer.on_partitions_revoked(tps)

        consumer._on_partitions_revoked.assert_called_once_with(
            tps)

    @pytest.mark.asyncio
    async def test_verify_subscription(self, *, consumer):
        await consumer.verify_subscription({TP('foo', 303)})

    def test_track_message(self, *, consumer, message):
        consumer._on_message_in = Mock(name='omin')
        consumer.track_message(message)

        assert message in consumer.unacked
        consumer._on_message_in.assert_called_once_with(
            message.tp, message.offset, message)

    @pytest.mark.parametrize('offset', [
            1, 303,
    ])
    def test_ack(self, offset, *, consumer, message):
        message.acked = False
        consumer.app = Mock(name='app')
        consumer.app.topics.acks_enabled_for.return_value = True
        consumer._committed_offset[message.tp] = 3
        message.offset = offset
        consumer._acked_index[message.tp] = set()
        consumer.ack(message)
        message.acked = False
        consumer.ack(message)

    def test_ack__already_acked(self, *, consumer, message):
        message.acked = True
        consumer.ack(message)

    def test_ack__disabled(self, *, consumer, message, app):
        message.acked = False
        app.topics = Mock(name='app.topics')
        app.topics.acks_enabled_for.return_value = False
        consumer.ack(message)

    @pytest.mark.asyncio
    async def test_wait_empty(self, *, consumer):
        consumer._unacked_messages = {Mock()}
        consumer.commit = Mock(name='commit')

        i = 0

        def on_commit():
            nonlocal i
            i += 1
            if i > 10:
                consumer._unacked_messages.clear()
            return done_future()
        consumer.commit.side_effect = on_commit

        await consumer.wait_empty()

    @pytest.mark.asyncio
    async def test_on_stop(self, *, consumer):
        consumer.app.conf.stream_wait_empty = False
        consumer._last_batch = 30.3
        await consumer.on_stop()
        assert consumer._last_batch is None

        consumer.app.conf.stream_wait_empty = True
        consumer.wait_empty = Mock(name='wait_empty')
        consumer.wait_empty.return_value = done_future()

        await consumer.on_stop()
        consumer.wait_empty.assert_called_once_with()
