import asyncio
import pytest
from faust import App
from faust.app._attached import Attachments
from faust.tables.manager import TableManager
from faust.transport.consumer import Consumer, Fetcher, ProducerSendError
from faust.transport.conductor import Conductor
from faust.types import Message, TP
from mode import Service
from mode.utils.mocks import ANY, AsyncMock, Mock, call

TP1 = TP('foo', 0)
TP2 = TP('foo', 1)


class test_Fetcher:

    @pytest.fixture
    def fetcher(self, *, app):
        return Fetcher(app)

    @pytest.mark.asyncio
    async def test_fetcher(self, *, fetcher, app):
        app.consumer = Mock(
            name='consumer',
            autospec=Consumer,
            _drain_messages=AsyncMock(),
        )
        # some weird pytest-asyncio thing
        fetcher.loop = asyncio.get_event_loop()
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
        return Mock(name='message', autospec=Message)

    def test_read_offset_default(self, *, consumer):
        assert consumer._read_offset[TP1] is None

    def test_committed_offset_default(self, *, consumer):
        assert consumer._committed_offset[TP1] is None

    def test_is_changelog_tp(self, *, app, consumer):
        app.tables = Mock(name='tables', autospec=TableManager)
        app.tables.changelog_topics = {'foo', 'bar'}
        assert consumer._is_changelog_tp(TP('foo', 31))
        assert consumer._is_changelog_tp(TP('bar', 0))
        assert not consumer._is_changelog_tp(TP('baz', 3))

    @pytest.mark.asyncio
    async def test_on_partitions_assigned(self, *, consumer):
        consumer._on_partitions_assigned = AsyncMock(name='opa')
        tps = {TP('foo', 0), TP('bar', 2)}
        await consumer.on_partitions_assigned(tps)

        consumer._on_partitions_assigned.assert_called_once_with(
            tps)

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, consumer):
        consumer._on_partitions_revoked = AsyncMock(name='opr')
        tps = {TP('foo', 0), TP('bar', 2)}
        await consumer.on_partitions_revoked(tps)

        consumer._on_partitions_revoked.assert_called_once_with(
            tps)

    def test_track_message(self, *, consumer, message):
        consumer._on_message_in = Mock(name='omin')
        consumer.track_message(message)

        assert message in consumer.unacked
        consumer._on_message_in.assert_called_once_with(
            message.tp, message.offset, message)

    @pytest.mark.parametrize('offset', [
        1,
        303,
    ])
    def test_ack(self, offset, *, consumer, message):
        message.acked = False
        consumer.app = Mock(name='app', autospec=App)
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
        app.topics = Mock(name='app.topics', autospec=Conductor)
        app.topics.acks_enabled_for.return_value = False
        consumer.ack(message)

    @pytest.mark.asyncio
    async def test_wait_empty(self, *, consumer):
        consumer._unacked_messages = {Mock(autospec=Message)}

        def on_commit():
            for _ in range(10):
                yield
            consumer._unacked_messages.clear()
        consumer.commit = AsyncMock(name='commit', side_effect=on_commit)

        await consumer.wait_empty()

    @pytest.mark.asyncio
    async def test_on_stop(self, *, consumer):
        consumer.app.conf.stream_wait_empty = False
        consumer._last_batch = 30.3
        await consumer.on_stop()
        assert consumer._last_batch is None

        consumer.app.conf.stream_wait_empty = True
        consumer.wait_empty = AsyncMock(name='wait_empty')

        await consumer.on_stop()
        consumer.wait_empty.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_force_commit(self, *, consumer):
        consumer.app = Mock(name='app', autospec=App)
        oci = consumer.app.sensors.on_commit_initiated
        occ = consumer.app.sensors.on_commit_completed
        consumer._commit_tps = AsyncMock(name='_commit_tps')
        consumer._acked = {
            TP1: [1, 2, 3, 4, 5],
        }
        consumer._committed_offset = {
            TP1: 2,
        }
        await consumer.force_commit({TP1})
        oci.assert_called_once_with(consumer)
        consumer._commit_tps.assert_called_once_with([TP1])
        occ.assert_called_once_with(consumer, oci())

    @pytest.mark.asyncio
    async def test_commit_tps(self, *, consumer):
        consumer._handle_attached = AsyncMock(name='_handle_attached')
        consumer._commit_offsets = AsyncMock(name='_commit_offsets')
        consumer._filter_committable_offsets = Mock(name='filt')
        consumer._filter_committable_offsets.return_value = {
            TP1: 4,
            TP2: 30,
        }
        await consumer._commit_tps({TP1, TP2})

        consumer._handle_attached.assert_called_once_with({
            TP1: 4,
            TP2: 30,
        })
        consumer._commit_offsets.assert_called_once_with({
            TP1: 4,
            TP2: 30,
        })

    @pytest.mark.asyncio
    async def test_commit_tps__ProducerSendError(self, *, consumer):
        consumer._handle_attached = Mock(name='_handle_attached')
        exc = consumer._handle_attached.side_effect = ProducerSendError()
        consumer.crash = AsyncMock(name='crash')
        consumer._filter_committable_offsets = Mock(name='filt')
        consumer._filter_committable_offsets.return_value = {
            TP1: 4,
            TP2: 30,
        }
        await consumer._commit_tps({TP1, TP2})

        consumer.crash.assert_called_once_with(exc)

    @pytest.mark.asyncio
    async def test_commit_tps__no_commitable(self, *, consumer):
        consumer._filter_commitable_offsets = Mock(name='filt')
        consumer._filter_commitable_offsets.return_value = {}
        await consumer._commit_tps({TP1, TP2})

    def test_filter_committable_offsets(self, *, consumer):
        consumer._acked = {
            TP1: [1, 2, 3, 4, 7, 8],
            TP2: [30, 31, 32, 33, 34, 35, 36, 40],
        }
        consumer._committed_offset = {
            TP1: 4,
            TP2: 30,
        }
        assert consumer._filter_committable_offsets({TP1, TP2}) == {
            TP2: 36,
        }

    @pytest.mark.asyncio
    async def test_handle_attached(self, *, consumer):
        consumer.app = Mock(
            name='app',
            autospec=App,
            _attachments=Mock(
                autospec=Attachments,
                publish_for_tp_offset=AsyncMock(),
            ),
            producer=Mock(
                autospec=Service,
                wait_many=AsyncMock(),
            ),
        )
        await consumer._handle_attached({
            TP1: 3003,
            TP2: 6006,
        })
        consumer.app._attachments.publish_for_tp_offset.coro.assert_has_calls([
            call(TP1, 3003),
            call(TP2, 6006),
        ])

        consumer.app.producer.wait_many.coro.assert_called_with(ANY)

    @pytest.mark.asyncio
    async def test_commit_offsets(self, *, consumer):
        consumer._commit = AsyncMock(name='_commit')
        await consumer._commit_offsets({
            TP1: 3003,
            TP2: 6006,
        })
        consumer._commit.assert_called_once_with({
            TP1: (3003, ''),
            TP2: (6006, ''),
        })

    def test_filter_tps_with_pending_acks(self, *, consumer):
        consumer._acked = {
            TP1: [1, 2, 3, 4, 5, 6],
            TP2: [3, 4, 5, 6],
        }
        assert list(consumer._filter_tps_with_pending_acks()) == [
            TP1, TP2,
        ]
        assert list(consumer._filter_tps_with_pending_acks([TP1])) == [
            TP1,
        ]
        assert list(consumer._filter_tps_with_pending_acks([TP1.topic])) == [
            TP1, TP2,
        ]

    @pytest.mark.parametrize('tp,offset,committed,should', [
        (TP1, 0, 0, False),
        (TP1, 1, 0, True),
        (TP1, 6, 8, False),
        (TP1, 100, 8, True),
    ])
    def test_should_commit(self, tp, offset, committed, should, *, consumer):
        consumer._committed_offset[tp] = committed
        assert consumer._should_commit(tp, offset) == should

    @pytest.mark.parametrize('tp,acked,expected_offset', [
        (TP1, [], None),
        (TP1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 10),
        (TP1, [1, 2, 3, 4, 5, 6, 7, 8, 10], 8),
        (TP1, [1, 2, 3, 4, 6, 7, 8, 10], 4),
        (TP1, [1, 3, 4, 6, 7, 8, 10], 1),
    ])
    def test_new_offset(self, tp, acked, expected_offset, *, consumer):
        consumer._acked[tp] = acked
        assert consumer._new_offset(tp) == expected_offset

    @pytest.mark.asyncio
    async def test_on_task_error(self, *, consumer):
        consumer.commit = AsyncMock(name='commit')
        await consumer.on_task_error(KeyError())
        consumer.commit.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_commit_handler(self, *, consumer):
        i = 0

        def on_sleep(secs):
            nonlocal i
            if i:
                consumer._stopped.set()
            i += 1

        consumer.sleep = AsyncMock(name='sleep', side_effect=on_sleep)
        consumer.commit = AsyncMock(name='commit')

        await consumer._commit_handler(consumer)
        consumer.sleep.coro.assert_has_calls([
            call(consumer.commit_interval),
            call(consumer.commit_interval),
        ])
        consumer.commit.assert_called_once_with()
