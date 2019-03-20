import asyncio
import pytest
from faust import App
from faust.app._attached import Attachments
from faust.exceptions import AlreadyConfiguredWarning
from faust.tables.manager import TableManager
from faust.transport.base import Producer, Transport
from faust.transport.consumer import (
    Consumer,
    ConsumerThread,
    Fetcher,
    ProducerSendError,
    ThreadDelegateConsumer,
    TransactionManager,
)
from faust.transport.conductor import Conductor
from faust.types import Message, TP
from mode import Service
from mode.threads import MethodQueue
from mode.utils.futures import done_future
from mode.utils.mocks import ANY, AsyncMock, Mock, call, patch

TP1 = TP('foo', 0)
TP2 = TP('foo', 1)
TP3 = TP('bar', 3)


class test_Fetcher:

    @pytest.fixture
    def consumer(self):
        return Mock(
            name='consumer',
            autospec=Consumer,
            _drain_messages=AsyncMock(),
        )

    @pytest.fixture
    def fetcher(self, *, app, consumer):
        fetcher = Fetcher(app)
        app.consumer = consumer
        # some weird pytest-asyncio thing
        fetcher.loop = asyncio.get_event_loop()
        return fetcher

    @pytest.mark.asyncio
    async def test_fetcher(self, *, fetcher, app):
        await fetcher._fetcher(fetcher)
        app.consumer._drain_messages.assert_called_once_with(fetcher)

    @pytest.mark.asyncio
    async def test_fetcher__raises_CancelledError(self, *, fetcher, app):
        app.consumer._drain_messages.side_effect = asyncio.CancelledError
        await fetcher._fetcher(fetcher)
        app.consumer._drain_messages.assert_called_once_with(fetcher)

    @pytest.mark.asyncio
    async def test_on_stop__no_drainer(self, *, fetcher):
        fetcher._drainer = None
        await fetcher.on_stop()

    @pytest.mark.asyncio
    async def test_on_stop__drainer_done(self, *, fetcher):
        fetcher._drainer = Mock(done=Mock(return_value=True))
        await fetcher.on_stop()

    @pytest.mark.asyncio
    async def test_on_stop__drainer_pending(self, *, fetcher):
        fetcher._drainer = Mock(done=Mock(return_value=False))
        with patch('asyncio.wait_for', AsyncMock()) as wait_for:
            await fetcher.on_stop()
            wait_for.assert_called_once_with(
                fetcher._drainer, timeout=1.0,
            )

    @pytest.mark.asyncio
    async def test_on_stop__drainer_raises_StopIteration(self, *, fetcher):
        fetcher._drainer = Mock(done=Mock(return_value=False))
        with patch('asyncio.wait_for', AsyncMock()) as wait_for:
            wait_for.side_effect = StopIteration()
            await fetcher.on_stop()
            wait_for.assert_called_once_with(
                fetcher._drainer, timeout=1.0,
            )

    @pytest.mark.asyncio
    async def test_on_stop__drainer_raises_CancelledError(self, *, fetcher):
        fetcher._drainer = Mock(done=Mock(return_value=False))
        with patch('asyncio.wait_for', AsyncMock()) as wait_for:
            wait_for.coro.side_effect = asyncio.CancelledError()
            await fetcher.on_stop()
            wait_for.assert_called_once_with(
                fetcher._drainer, timeout=1.0,
            )

    @pytest.mark.asyncio
    async def test_on_stop__drainer_raises_TimeoutError(self, *, fetcher):
        fetcher._drainer = Mock(done=Mock(return_value=False))
        with patch('asyncio.wait_for', AsyncMock()) as wait_for:
            wait_for.coro.side_effect = [
                asyncio.TimeoutError(),
                asyncio.TimeoutError(),
                None,
            ]
            await fetcher.on_stop()
            wait_for.assert_called_with(
                fetcher._drainer, timeout=1.0,
            )
            assert wait_for.call_count == 3


class test_TransactionManager:

    @pytest.fixture()
    def consumer(self):
        return Mock(
            name='consumer',
            spec=Consumer,
        )

    @pytest.fixture()
    def producer(self):
        return Mock(
            name='producer',
            spec=Producer,
            create_topic=AsyncMock(),
            stop_transaction=AsyncMock(),
            maybe_begin_transaction=AsyncMock(),
            commit_transactions=AsyncMock(),
            send=AsyncMock(),
            flush=AsyncMock(),
        )

    @pytest.fixture()
    def transport(self, *, app):
        return Mock(
            name='transport',
            spec=Transport,
            app=app,
        )

    @pytest.fixture()
    def manager(self, *, consumer, producer, transport):
        return TransactionManager(
            transport,
            consumer=consumer,
            producer=producer,
        )

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, manager):
        manager.flush = AsyncMock()
        await manager.on_partitions_revoked({TP1})
        manager.flush.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_rebalance(self, *, manager):
        TP3_group = 0
        TP2_group = 2
        manager.app.assignor._topic_groups = {
            TP3.topic: TP3_group,
            TP2.topic: TP2_group,
        }

        assert TP3.topic != TP2.topic  # must be different topics
        manager._stop_transactions = AsyncMock()
        manager._start_transactions = AsyncMock()

        assigned = {TP2}
        revoked = {TP3}
        newly_assigned = {TP2}
        await manager.on_rebalance(assigned, revoked, newly_assigned)

        manager._stop_transactions.assert_called_once_with(
            [f'{TP3_group}-{TP3.partition}'])
        manager._start_transactions.assert_called_once_with(
            [f'{TP2_group}-{TP2.partition}'])

        await manager.on_rebalance(set(), set(), set())

    @pytest.mark.asyncio
    async def test__stop_transactions(self, *, manager, producer):
        await manager._stop_transactions(['0-0', '1-0'])
        producer.stop_transaction.assert_has_calls([
            call('0-0'),
            call.coro('0-0'),
            call('1-0'),
            call.coro('1-0'),
        ])

    @pytest.mark.asyncio
    async def test_start_transactions(self, *, manager, producer):
        manager._start_new_producer = AsyncMock()
        await manager._start_transactions(['0-0', '1-0'])
        producer.maybe_begin_transaction.assert_has_calls([
            call('0-0'),
            call.coro('0-0'),
            call('1-0'),
            call.coro('1-0'),
        ])

    @pytest.mark.asyncio
    async def test_send(self, *, manager, producer):
        manager.app.assignor._topic_groups = {
            't': 3,
        }
        manager.consumer.key_partition.return_value = 1

        await manager.send(
            't', 'k', 'v', partition=None, headers=None, timestamp=None)
        manager.consumer.key_partition.assert_called_once_with('t', 'k', None)
        producer.send.assert_called_once_with(
            't', 'k', 'v', 1, None, None, transactional_id='3-1',
        )

    @pytest.mark.asyncio
    async def test_send_and_wait(self, *, manager):
        on_send = Mock()

        async def send(*args, **kwargs):
            on_send(*args, **kwargs)
            return done_future()
        manager.send = send

        await manager.send_and_wait('t', 'k', 'v', 3, 43.2, {})
        on_send.assert_called_once_with(
            't', 'k', 'v', 3, 43.2, {},
        )

    @pytest.mark.asyncio
    async def test_commit(self, *, manager, producer):
        manager.app.assignor._topic_groups = {
            'foo': 1,
            'bar': 2,
        }
        await manager.commit(
            {
                TP('foo', 0): 3003,
                TP('bar', 0): 3004,
                TP('foo', 3): 4004,
                TP('foo', 1): 4005,
            },
            start_new_transaction=False,
        )
        producer.commit_transactions.assert_called_once_with(
            {
                '1-0': {
                    TP('foo', 0): 3003,
                },
                '1-3': {
                    TP('foo', 3): 4004,
                },
                '1-1': {
                    TP('foo', 1): 4005,
                },
                '2-0': {
                    TP('bar', 0): 3004,
                },
            },
            'testid',
            start_new_transaction=False,
        )

    @pytest.mark.asyncio
    async def test_commit__empty(self, *, manager):
        await manager.commit({}, start_new_transaction=False)

    def test_key_partition(self, *, manager):
        with pytest.raises(NotImplementedError):
            manager.key_partition('topic', 'key')

    @pytest.mark.asyncio
    async def test_flush(self, *, manager, producer):
        await manager.flush()
        producer.flush.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_create_topic(self, *, manager):
        await manager.create_topic(
            topic='topic',
            partitions=100,
            replication=3,
            config={'C': 1},
            timeout=30.0,
            retention=40.0,
            compacting=True,
            deleting=True,
            ensure_created=True,
        )
        manager.producer.create_topic.assert_called_once_with(
            'topic', 100, 3,
            config={'C': 1},
            timeout=30.0,
            retention=40.0,
            compacting=True,
            deleting=True,
            ensure_created=True,
        )

    def test_supports_headers(self, *, manager):
        ret = manager.supports_headers()
        assert ret is manager.producer.supports_headers.return_value


class MockedConsumerAbstractMethods:

    def assignment(self):
        return self.current_assignment

    def position(self, *args, **kwargs):
        ...

    async def create_topic(self, *args, **kwargs):
        ...

    def earliest_offsets(self, *args, **kwargs):
        ...

    def highwater(self, *args, **kwargs):
        ...

    def highwaters(self, *args, **kwargs):
        ...

    async def _getmany(self, *args, **kwargs):
        ...

    async def _seek(self, *args, **kwargs):
        ...

    def _to_message(self, *args, **kwargs):
        ...

    async def seek_to_committed(self, *args, **kwargs):
        ...

    async def seek_wait(self, *args, **kwargs):
        ...

    async def subscribe(self, *args, **kwargs):
        ...

    async def seek_to_beginning(self, *args, **kwargs):
        ...

    async def _commit(self, offsets) -> bool:
        ...

    def topic_partitions(self, topic):
        ...

    def _new_topicpartition(self, topic, partition) -> TP:
        return TP(topic, partition)

    def key_partition(self, *args, **kwargs):
        ...


class MyConsumer(MockedConsumerAbstractMethods, Consumer):

    def __init__(self, *args, **kwargs) -> None:
        self.current_assignment = set()
        super().__init__(*args, **kwargs)


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

    def test_on_init_dependencies__default(self, *, consumer):
        consumer.in_transaction = False
        assert consumer.on_init_dependencies() == []

    def test_on_init_dependencies__exactly_once(self, *, consumer):
        consumer.in_transaction = True
        assert consumer.on_init_dependencies() == [consumer.transactions]

    @pytest.mark.asyncio
    async def test_getmany__stopped_after_wait(self, *, consumer):
        consumer._wait_next_records = AsyncMock()

        async def on_wait(timeout):
            consumer._stopped.set()
            return None, None

        consumer._wait_next_records.side_effect = on_wait
        assert [a async for a in consumer.getmany(1.0)] == []

    @pytest.mark.asyncio
    async def test_getmany__flow_inactive(self, *, consumer):
        consumer._wait_next_records = AsyncMock(return_value=(
            {TP1: ['A', 'B', 'C']},
            {TP1},
        ))
        consumer.flow_active = False
        assert [a async for a in consumer.getmany(1.0)] == []

    @pytest.mark.asyncio
    async def test_getmany__flow_inactive2(self, *, consumer):
        consumer._wait_next_records = AsyncMock(return_value=(
            {TP1: ['A', 'B', 'C'], TP2: ['D']},
            {TP1},
        ))
        consumer.scheduler = Mock()

        def se(records):
            for value in records.items():
                yield value
                consumer.flow_active = False
        consumer.scheduler.iterate.side_effect = se

        consumer.flow_active = True
        res = [a async for a in consumer.getmany(1.0)]
        assert res
        assert len(res) == 1

    @pytest.mark.asyncio
    async def test_getmany(self, *, consumer):
        def to_message(tp, record):
            return record
        consumer._to_message = to_message
        self._setup_records(
            consumer,
            active_partitions={TP1, TP2},
            records={
                TP1: ['A', 'B', 'C'],
                TP2: ['D', 'E', 'F', 'G'],
                TP3: ['H', 'I', 'J'],
            },
        )
        assert not consumer.should_stop
        consumer.flow_active = False
        consumer.can_resume_flow.set()
        assert [a async for a in consumer.getmany(1.0)] == []
        assert not consumer.should_stop
        consumer.flow_active = True
        assert [a async for a in consumer.getmany(1.0)] == [
            (TP1, 'A'),
            (TP2, 'D'),
            (TP1, 'B'),
            (TP2, 'E'),
            (TP1, 'C'),
            (TP2, 'F'),
            (TP2, 'G'),
        ]

    @pytest.mark.asyncio
    async def test__wait_next_records(self, *, consumer):
        self._setup_records(
            consumer,
            active_partitions={TP1},
            records={
                TP1: ['A', 'B', 'C'],
            },
        )
        ret = await consumer._wait_next_records(1.0)
        assert ret == ({TP1: ['A', 'B', 'C']}, {TP1})

    @pytest.mark.asyncio
    async def test__wait_next_records__flow_inactive(self, *, consumer):
        self._setup_records(consumer, {TP1}, flow_active=False)
        consumer.can_resume_flow.set()
        consumer.wait = AsyncMock()
        await consumer._wait_next_records(1.0)
        consumer.wait.assert_called_once_with(consumer.can_resume_flow)

    @pytest.mark.asyncio
    async def test__wait_next_records__no_active_tps(self, *, consumer):
        self._setup_records(consumer, set())
        consumer.sleep = AsyncMock()
        ret = await consumer._wait_next_records(1.0)
        consumer.sleep.assert_called_once_with(1)
        assert ret == ({}, set())

    def _setup_records(self, consumer, active_partitions,
                       records=None,
                       flow_active=True):
        consumer.flow_active = flow_active
        consumer._active_partitions = active_partitions
        consumer._getmany = AsyncMock(
            return_value={} if records is None else records,
        )

    @pytest.mark.asyncio
    async def test__wait_for_ack(self, *, consumer):

        async def set_ack():
            await asyncio.sleep(0)
            consumer._waiting_for_ack.set_result(None)

        await asyncio.gather(
            consumer._wait_for_ack(timeout=1.0),
            set_ack(),
        )

    @pytest.mark.asyncio
    async def test_on_restart(self, *, consumer):
        consumer._reset_state = Mock()
        consumer.on_init = Mock()
        await consumer.on_restart()
        consumer._reset_state.assert_called_once_with()
        consumer.on_init.assert_called_once_with()

    def test__get_active_partitions__when_empty(self, *, consumer):
        consumer._active_partitions = None
        consumer.assignment = Mock(return_value={TP1})
        assert consumer._get_active_partitions() == {TP1}

    def test__get_active_partitions__when_set(self, *, consumer):
        consumer._active_partitions = {TP1, TP2}
        consumer.assignment = Mock(return_value={TP1})
        assert consumer._get_active_partitions() == {TP1, TP2}

    @pytest.mark.asyncio
    async def test_perform_seek(self, *, consumer):
        consumer._read_offset.update(TP1=3001, TP2=3002)
        consumer._committed_offset.update(TP1=301, TP2=302)
        consumer.seek_to_committed = AsyncMock(return_value={
            TP1: 4001,
            TP2: 0,
        })

        await consumer.perform_seek()

        assert consumer._read_offset[TP1] == 4001
        assert consumer._read_offset[TP2] == 0

        assert consumer._committed_offset[TP1] == 4001
        assert consumer._committed_offset[TP2] is None

    @pytest.mark.asyncio
    async def test_seek(self, *, consumer):
        consumer._last_batch = 123.3
        consumer._read_offset[TP1] = 301
        consumer._seek = AsyncMock()

        await consumer.seek(TP1, 401)

        assert consumer._last_batch is None
        assert consumer._read_offset[TP1] == 401
        consumer._seek.assert_called_once_with(TP1, 401)

    def test_stop_flow(self, *, consumer):
        consumer.flow_active = True
        consumer.can_resume_flow.set()

        consumer.stop_flow()

        assert not consumer.flow_active
        assert not consumer.can_resume_flow.is_set()

    def test_resume_flow(self, *, consumer):
        consumer.flow_active = False
        consumer.can_resume_flow.clear()

        consumer.resume_flow()

        assert consumer.flow_active
        assert consumer.can_resume_flow.is_set()

    def test_pause_partitions(self, *, consumer):
        consumer._paused_partitions.clear()
        consumer._active_partitions = {TP1, TP2}

        consumer.pause_partitions([TP2])
        assert consumer._active_partitions == {TP1}
        assert consumer._paused_partitions == {TP2}

    def test_resume_partitions(self, *, consumer):
        consumer._paused_partitions = {TP2}
        consumer._active_partitions = {TP1}

        consumer.resume_partitions([TP2])
        assert consumer._active_partitions == {TP1, TP2}
        assert not consumer._paused_partitions

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
    async def test_on_partitions_revoked(self, *, consumer):
        consumer._on_partitions_revoked = AsyncMock(name='opr')
        tps = {TP('foo', 0), TP('bar', 2)}
        await consumer.on_partitions_revoked(tps)

        consumer._on_partitions_revoked.assert_called_once_with(
            tps)

    @pytest.mark.asyncio
    async def test_on_partitions_revoked__updates_active(self, *, consumer):
        consumer._active_partitions = {TP('foo', 0)}
        consumer._on_partitions_revoked = AsyncMock(name='opr')
        tps = {TP('foo', 0), TP('bar', 2)}
        await consumer.on_partitions_revoked(tps)

        consumer._on_partitions_revoked.assert_called_once_with(
            tps)

        assert not consumer._active_partitions

    @pytest.mark.asyncio
    async def test_on_partitions_assigned(self, *, consumer):
        consumer._on_partitions_assigned = AsyncMock(name='opa')
        tps = {TP('foo', 0), TP('bar', 2)}
        await consumer.on_partitions_assigned(tps)

        consumer._on_partitions_assigned.assert_called_once_with(
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
        consumer._wait_for_ack = AsyncMock()

        def on_commit(start_new_transaction=True):
            for _ in range(10):
                yield
            if consumer.commit.call_count == 3:
                consumer._unacked_messages.clear()
        consumer.commit = AsyncMock(name='commit', side_effect=on_commit)

        await consumer.wait_empty()

    @pytest.mark.asyncio
    async def test_wait_empty__when_stopped(self, *, consumer):
        consumer._stopped.set()
        await consumer.wait_empty()

    @pytest.mark.asyncio
    async def test_on_stop(self, *, consumer):
        consumer.app.conf.stream_wait_empty = False
        consumer._last_batch = 30.3
        await consumer.on_stop()
        assert consumer._last_batch is None

        with pytest.warns(AlreadyConfiguredWarning):
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
        consumer._commit_tps.assert_called_once_with(
            [TP1],
            start_new_transaction=True,
        )
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
        await consumer._commit_tps(
            {TP1, TP2},
            start_new_transaction=False,
        )

        consumer._handle_attached.assert_called_once_with({
            TP1: 4,
            TP2: 30,
        })
        consumer._commit_offsets.assert_called_once_with(
            {TP1: 4, TP2: 30},
            start_new_transaction=False,
        )

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
        await consumer._commit_tps(
            {TP1, TP2},
            start_new_transaction=True,
        )

        consumer.crash.assert_called_once_with(exc)

    @pytest.mark.asyncio
    async def test_commit_tps__no_commitable(self, *, consumer):
        consumer._filter_commitable_offsets = Mock(name='filt')
        consumer._filter_commitable_offsets.return_value = {}
        await consumer._commit_tps(
            {TP1, TP2},
            start_new_transaction=True,
        )

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
        att = consumer.app._attachments
        att.publish_for_tp_offset.coro.return_value = None
        await consumer._handle_attached({
            TP1: 3003,
            TP2: 6006,
        })

    @pytest.mark.asyncio
    async def test_commit_offsets(self, *, consumer):
        consumer._commit = AsyncMock(name='_commit')
        consumer.current_assignment.update({TP1, TP2})
        await consumer._commit_offsets({
            TP1: 3003,
            TP2: 6006,
        })
        consumer._commit.assert_called_once_with({
            TP1: 3003,
            TP2: 6006,
        })

    @pytest.mark.asyncio
    async def test_commit_offsets__did_not_commit(self, *, consumer):
        consumer.in_transaction = False
        consumer._commit = AsyncMock(return_value=False)
        consumer.current_assignment.update({TP1, TP2})
        consumer.app.tables = Mock(name='app.tables')
        await consumer._commit_offsets({
            TP1: 3003,
            TP2: 6006,
            TP3: 7007,
        })
        consumer.app.tables.on_commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_commit_offsets__in_transaction(self, *, consumer):
        consumer.in_transaction = True
        consumer.transactions.commit = AsyncMock()
        consumer.current_assignment.update({TP1, TP2})
        ret = await consumer._commit_offsets({
            TP1: 3003,
            TP2: 6006,
            TP3: 7007,
        })
        consumer.transactions.commit.assert_called_once_with(
            {TP1: 3003, TP2: 6006},
            start_new_transaction=True,
        )
        assert ret is consumer.transactions.commit.coro()

    @pytest.mark.asyncio
    async def test_commit_offsets__no_commitable_offsets(self, *, consumer):
        consumer.current_assignment.clear()
        assert not await consumer._commit_offsets({
            TP1: 3003,
            TP2: 6006,
            TP3: 7007,
        })

    @pytest.mark.asyncio
    async def test_commit__already_committing(self, *, consumer):
        consumer.maybe_wait_for_commit_to_finish = AsyncMock(
            return_value=True)
        assert not await consumer.commit()

    @pytest.mark.asyncio
    async def test_commit(self, *, consumer):
        topics = {'foo', 'bar'}
        start_new_transaction = False
        consumer.maybe_wait_for_commit_to_finish = AsyncMock(
            return_value=False)
        consumer.force_commit = AsyncMock()
        ret = await consumer.commit(
            topics,
            start_new_transaction=start_new_transaction,
        )
        consumer.force_commit.assert_called_once_with(
            topics,
            start_new_transaction=start_new_transaction,
        )
        assert ret is consumer.force_commit.coro()
        assert consumer._commit_fut is None

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

    def test_close(self, *, consumer):
        consumer.close()


class test_ConsumerThread:

    class MyConsumerThread(MockedConsumerAbstractMethods, ConsumerThread):

        async def getmany(self, *args, **kwargs):
            yield None, None

        def pause_partitions(self, *args, **kwargs):
            ...

        def resume_partitions(self, *args, **kwargs):
            ...

        def stop_flow(self, *args, **kwargs):
            ...

        def resume_flow(self, *args, **kwargs):
            ...

        async def commit(self, *args, **kwargs):
            ...

        async def perform_seek(self, *args, **kwargs):
            ...

        async def seek(self, *args, **kwargs):
            ...

    @pytest.fixture
    def consumer(self):
        return Mock(
            name='consumer',
            spec=Consumer,
            threadsafe_partitions_revoked=AsyncMock(),
            threadsafe_partitions_assigned=AsyncMock(),
            transport=Mock(),
        )

    @pytest.fixture
    def thread(self, *, consumer):
        return self.MyConsumerThread(consumer)

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, thread, consumer):
        await thread.on_partitions_revoked({TP1, TP2})
        consumer.threadsafe_partitions_revoked.assert_called_once_with(
            thread.thread_loop, {TP1, TP2},
        )

    @pytest.mark.asyncio
    async def test_on_partitions_assigned(self, *, thread, consumer):
        await thread.on_partitions_assigned({TP1, TP2})
        consumer.threadsafe_partitions_assigned.assert_called_once_with(
            thread.thread_loop, {TP1, TP2},
        )


class test_ThreadDelegateConsumer:

    class TestThreadDelegateConsumer(ThreadDelegateConsumer):

        def _new_consumer_thread(self):
            return Mock(
                name='consumer._thread',
                spec=Consumer,
                getmany=AsyncMock(),
                subscribe=AsyncMock(),
                seek_to_committed=AsyncMock(),
                position=AsyncMock(),
                seek_wait=AsyncMock(),
                seek=AsyncMock(),
                earliest_offsets=AsyncMock(),
                highwaters=AsyncMock(),
                commit=AsyncMock(),
            )

        def _new_topicpartition(self, *args, **kwargs):
            return TP(*args, **kwargs)

        def _to_message(self, *args, **kwargs):
            return (args, kwargs)

        def create_topic(self, *args, **kwargs):
            return

    @pytest.fixture
    def message_callback(self):
        return Mock(name='message_callback')

    @pytest.fixture
    def partitions_revoked_callback(self):
        return Mock(name='partitions_revoked_callback')

    @pytest.fixture
    def partitions_assigned_callback(self):
        return Mock(name='partitions_assigned_callback')

    @pytest.fixture
    def consumer(self, *,
                 app,
                 message_callback,
                 partitions_revoked_callback,
                 partitions_assigned_callback):
        consumer = self.TestThreadDelegateConsumer(
            app.transport,
            callback=message_callback,
            on_partitions_revoked=partitions_revoked_callback,
            on_partitions_assigned=partitions_assigned_callback,
        )

        async def _call(*args, **kwargs):
            consumer._method_queue._call(*args, **kwargs)
            return done_future()

        consumer._method_queue = Mock(
            name='_method_queue',
            spec=MethodQueue,
            call=_call,
            _call=Mock(),
        )
        return consumer

    @pytest.mark.asyncio
    async def test_threadsafe_partitions_revoked(self, *, consumer):
        loop = Mock(name='loop')
        await consumer.threadsafe_partitions_revoked(loop, {})
        loop.create_future.assert_called_once_with()
        consumer._method_queue._call.assert_called_once_with(
            loop.create_future(),
            consumer.on_partitions_revoked,
            {},
        )

    @pytest.mark.asyncio
    async def test_threadsafe_partitions_assigned(self, *, consumer):
        loop = Mock(name='loop')
        await consumer.threadsafe_partitions_assigned(loop, {})
        loop.create_future.assert_called_once_with()
        consumer._method_queue._call.assert_called_once_with(
            loop.create_future(),
            consumer.on_partitions_assigned,
            {},
        )

    @pytest.mark.asyncio
    async def test__getmany(self, *, consumer):
        ret = await consumer._getmany({TP1, TP2}, timeout=30.334)
        consumer._thread.getmany.assert_called_once_with(
            {TP1, TP2}, 30.334,
        )
        assert ret is consumer._thread.getmany.coro.return_value

    @pytest.mark.asyncio
    async def test_subscribe(self, *, consumer):
        await consumer.subscribe(topics=['a', 'b', 'c'])
        consumer._thread.subscribe.assert_called_once_with(
            topics=['a', 'b', 'c'],
        )

    @pytest.mark.asyncio
    async def test_seek_to_committed(self, *, consumer):
        ret = await consumer.seek_to_committed()
        consumer._thread.seek_to_committed.assert_called_once_with()
        assert ret is consumer._thread.seek_to_committed.coro.return_value

    @pytest.mark.asyncio
    async def test_position(self, *, consumer):
        ret = await consumer.position(TP1)
        consumer._thread.position.assert_called_once_with(TP1)
        assert ret is consumer._thread.position.coro.return_value

    @pytest.mark.asyncio
    async def test_seek_wait(self, *, consumer):
        await consumer.seek_wait({TP1: 301})
        consumer._thread.seek_wait.assert_called_once_with({TP1: 301})

    @pytest.mark.asyncio
    async def test__seek(self, *, consumer):
        await consumer._seek(TP1, 302)
        consumer._thread.seek.assert_called_once_with(TP1, 302)

    def test_assignment(self, *, consumer):
        ret = consumer.assignment()
        consumer._thread.assignment.assert_called_once_with()
        assert ret is consumer._thread.assignment()

    def test_highwater(self, *, consumer):
        ret = consumer.highwater(TP2)
        consumer._thread.highwater.assert_called_once_with(TP2)
        assert ret is consumer._thread.highwater()

    def test_topic_partitions(self, *, consumer):
        ret = consumer.topic_partitions(TP2.topic)
        consumer._thread.topic_partitions.assert_called_once_with(TP2.topic)
        assert ret is consumer._thread.topic_partitions()

    @pytest.mark.asyncio
    async def test_earliest_offsets(self, *, consumer):
        ret = await consumer.earliest_offsets(TP1, TP2)
        consumer._thread.earliest_offsets.assert_called_once_with(TP1, TP2)
        assert ret is consumer._thread.earliest_offsets.coro.return_value

    @pytest.mark.asyncio
    async def test_highwaters(self, *, consumer):
        ret = await consumer.highwaters(TP1, TP2)
        consumer._thread.highwaters.assert_called_once_with(TP1, TP2)
        assert ret is consumer._thread.highwaters.coro.return_value

    @pytest.mark.asyncio
    async def test_commit(self, *, consumer):
        ret = await consumer._commit({TP1: 301, TP2: 302})
        consumer._thread.commit.assert_called_once_with(
            {TP1: 301, TP2: 302},
        )
        assert ret is consumer._thread.commit.coro.return_value

    @pytest.mark.asyncio
    async def test_maybe_wait_for_commit_to_finish(self, *, loop, consumer):
        consumer._commit_fut = None
        assert not await consumer.maybe_wait_for_commit_to_finish()
        consumer._commit_fut = loop.create_future()
        consumer._commit_fut.cancel()
        assert not await consumer.maybe_wait_for_commit_to_finish()
        consumer._commit_fut = loop.create_future()
        consumer._commit_fut.set_result(None)
        assert await consumer.maybe_wait_for_commit_to_finish()

    @pytest.mark.asyncio
    async def test_close(self, *, consumer):
        consumer.close()
        consumer._thread.close.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_key_partition(self, *, consumer):
        ret = consumer.key_partition('topic', 'key', partition=3)
        consumer._thread.key_partition.assert_called_once_with(
            'topic', 'key', partition=3,
        )
        assert ret is consumer._thread.key_partition()
