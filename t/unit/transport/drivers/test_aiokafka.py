import aiokafka
import faust
import opentracing
import pytest
import random
import string
from contextlib import contextmanager
from typing import Optional

from aiokafka.errors import CommitFailedError, IllegalStateError, KafkaError
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from opentracing.ext import tags
from faust import auth
from faust.exceptions import ImproperlyConfigured, NotReady
from faust.sensors.monitor import Monitor
from faust.transport.drivers import aiokafka as mod
from faust.transport.drivers.aiokafka import (
    AIOKafkaConsumerThread,
    Consumer,
    ConsumerNotStarted,
    ConsumerRebalanceListener,
    ConsumerStoppedError,
    Producer,
    ProducerSendError,
    TOPIC_LENGTH_MAX,
    Transport,
    credentials_to_aiokafka_auth,
    server_list,
)
from faust.types import TP
from mode.utils.futures import done_future
from mode.utils.mocks import ANY, AsyncMock, MagicMock, Mock, call, patch

TP1 = TP('topic', 23)
TP2 = TP('topix', 23)

TESTED_MODULE = 'faust.transport.drivers.aiokafka'


@pytest.fixture()
def thread():
    return Mock(
        name='thread',
        create_topic=AsyncMock(),
    )


@pytest.fixture()
def consumer(*,
             thread,
             app,
             callback,
             on_partitions_revoked,
             on_partitions_assigned):
    consumer = Consumer(
        app.transport,
        callback=callback,
        on_partitions_revoked=on_partitions_revoked,
        on_partitions_assigned=on_partitions_assigned,
    )
    consumer._thread = thread
    return consumer


@pytest.fixture()
def callback():
    return Mock(name='callback')


@pytest.fixture()
def on_partitions_revoked():
    return Mock(name='on_partitions_revoked')


@pytest.fixture()
def on_partitions_assigned():
    return Mock(name='on_partitions_assigned')


class test_ConsumerRebalanceListener:

    @pytest.fixture()
    def handler(self, *, thread):
        return ConsumerRebalanceListener(thread)

    @pytest.fixture()
    def thread(self):
        return Mock(
            name='thread',
            on_partitions_assigned=AsyncMock(),
            on_partitions_revoked=AsyncMock(),
        )

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, handler, thread):
        await handler.on_partitions_revoked([
            TopicPartition('A', 0),
            TopicPartition('B', 3),
        ])
        thread.on_partitions_revoked.assert_called_once_with({
            TP('A', 0), TP('B', 3),
        })

    @pytest.mark.asyncio
    async def test_on_partitions_assigned(self, *, handler, thread):
        await handler.on_partitions_assigned([
            TopicPartition('A', 0),
            TopicPartition('B', 3),
        ])
        thread.on_partitions_assigned.assert_called_once_with({
            TP('A', 0), TP('B', 3),
        })


class test_Consumer:

    @pytest.fixture()
    def thread(self):
        return Mock(
            name='thread',
            create_topic=AsyncMock(),
        )

    @pytest.fixture()
    def consumer(self, *,
                 thread,
                 app,
                 callback,
                 on_partitions_revoked,
                 on_partitions_assigned):
        consumer = Consumer(
            app.transport,
            callback=callback,
            on_partitions_revoked=on_partitions_revoked,
            on_partitions_assigned=on_partitions_assigned,
        )
        consumer._thread = thread
        return consumer

    @pytest.fixture()
    def callback(self):
        return Mock(name='callback')

    @pytest.fixture()
    def on_partitions_revoked(self):
        return Mock(name='on_partitions_revoked')

    @pytest.fixture()
    def on_partitions_assigned(self):
        return Mock(name='on_partitions_assigned')

    @pytest.mark.asyncio
    async def test_create_topic(self, *, consumer, thread):
        await consumer.create_topic(
            'topic', 30, 3,
            timeout=40.0,
            retention=50.0,
            compacting=True,
            deleting=True,
            ensure_created=True,
        )
        thread.create_topic.assert_called_once_with(
            'topic',
            30,
            3,
            config=None,
            timeout=40.0,
            retention=50.0,
            compacting=True,
            deleting=True,
            ensure_created=True,
        )

    def test__new_topicpartition(self, *, consumer):
        tp = consumer._new_topicpartition('t', 3)
        assert isinstance(tp, TopicPartition)
        assert tp.topic == 't'
        assert tp.partition == 3

    def test__to_message(self, *, consumer):
        record = self.mock_record(
            timestamp=3000,
            headers=[('a', b'b')],
        )
        m = consumer._to_message(TopicPartition('t', 3), record)
        assert m.topic == record.topic
        assert m.partition == record.partition
        assert m.offset == record.offset
        assert m.timestamp == 3.0
        assert m.headers == record.headers
        assert m.key == record.key
        assert m.value == record.value
        assert m.checksum == record.checksum
        assert m.serialized_key_size == record.serialized_key_size
        assert m.serialized_value_size == record.serialized_value_size

    def test__to_message__no_timestamp(self, *, consumer):
        record = self.mock_record(timestamp=None)
        m = consumer._to_message(TopicPartition('t', 3), record)
        assert m.timestamp is None

    def mock_record(self,
                    topic='t',
                    partition=3,
                    offset=1001,
                    timestamp=None,
                    timestamp_type=1,
                    headers=None,
                    key=b'key',
                    value=b'value',
                    checksum=312,
                    serialized_key_size=12,
                    serialized_value_size=40,
                    **kwargs):
        return Mock(
            name='record',
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp=timestamp,
            timestamp_type=timestamp_type,
            headers=headers,
            key=key,
            value=value,
            checksum=checksum,
            serialized_key_size=serialized_key_size,
            serialized_value_size=serialized_value_size,
        )

    @pytest.mark.asyncio
    async def test_on_stop(self, *, consumer):
        consumer.transport._topic_waiters = {'topic': Mock()}
        await consumer.on_stop()
        assert not consumer.transport._topic_waiters


class AIOKafkaConsumerThreadFixtures:

    @pytest.fixture()
    def cthread(self, *, consumer):
        return AIOKafkaConsumerThread(consumer)

    @pytest.fixture()
    def tracer(self, *, app):
        tracer = app.tracer = Mock(name='tracer')
        tobj = tracer.get_tracer.return_value

        def start_span(operation_name=None, **kwargs):
            span = opentracing.Span(
                tracer=tobj,
                context=opentracing.SpanContext(),
            )

            if operation_name is not None:
                span.operation_name = operation_name
                assert span.operation_name == operation_name
            return span

        tobj.start_span = start_span
        return tracer

    @pytest.fixture()
    def _consumer(self):
        return Mock(
            name='AIOKafkaConsumer',
            autospec=aiokafka.AIOKafkaConsumer,
            start=AsyncMock(),
            stop=AsyncMock(),
            commit=AsyncMock(),
            position=AsyncMock(),
            end_offsets=AsyncMock(),
        )

    @pytest.fixture()
    def now(self):
        return 1201230410

    @pytest.fixture()
    def tp(self):
        return TP('foo', 30)

    @pytest.fixture()
    def aiotp(self, *, tp):
        return TopicPartition(tp.topic, tp.partition)

    @pytest.fixture()
    def logger(self, *, cthread):
        cthread.log = Mock(name='cthread.log')
        return cthread.log


class test_verify_event_path_base(AIOKafkaConsumerThreadFixtures):

    last_request: Optional[float] = None
    last_response: Optional[float] = None
    highwater: int = 1
    committed_offset: int = 1
    acks_enabled: bool = False
    stream_inbound: Optional[float] = None
    last_commit: Optional[float] = None
    expected_message: Optional[str] = None
    has_monitor = True

    def _set_started(self, t):
        self._cthread.time_started = t

    def _set_last_request(self, last_request):
        self.__consumer.records_last_request[self._aiotp] = last_request

    def _set_last_response(self, last_response):
        self.__consumer.records_last_response[self._aiotp] = last_response

    def _set_stream_inbound(self, inbound_time):
        self._app.monitor.stream_inbound_time[self._tp] = inbound_time

    def _set_last_commit(self, commit_time):
        self._cthread.tp_last_committed_at[self._tp] = commit_time

    @pytest.fixture(autouse=True)
    def aaaa_setup_attributes(self, *,
                              app,
                              cthread,
                              _consumer,
                              now,
                              tp,
                              aiotp):
        self._app = app
        self._tp = tp
        self._aiotp = aiotp
        self._now = now
        self._cthread = cthread
        self.__consumer = _consumer

    @pytest.fixture(autouse=True)
    def setup_consumer(self, *, app, cthread, _consumer, now, tp, aiotp):
        assert self._tp is tp
        assert self._aiotp is aiotp
        # patch self.acks_enabledc
        app.topics.acks_enabled_for = Mock(name='acks_enabled_for')
        app.topics.acks_enabled_for.return_value = self.acks_enabled

        # patch consumer.time_started
        self._set_started(now)

        # connect underlying AIOKafkaConsumer object.
        cthread._consumer = _consumer

        # patch AIOKafkaConsumer.records_last_request to self.last_request
        _consumer.records_last_request = {}
        if self.last_request is not None:
            self._set_last_request(self.last_request)

        # patch AIOKafkaConsumer.records_last_response to self.last_response
        _consumer.records_last_response = {}
        if self.last_response is not None:
            self._set_last_response(self.last_response)

        # patch app.monitor
        if self.has_monitor:
            cthread.consumer.app.monitor = Mock(name='monitor', spec=Monitor)
            app.monitor = cthread.consumer.app.monitor
            app.monitor = Mock(name='monitor', spec=Monitor)
            # patch monitor.stream_inbound_time
            # this is the time when a stream last processed a record
            # for tp
            app.monitor.stream_inbound_time = {}
            self._set_stream_inbound(self.stream_inbound)
        else:
            app.monitor = None

        # patch highwater
        cthread.highwater = Mock(name='highwater')
        cthread.highwater.return_value = self.highwater

        # patch committed offset
        cthread.consumer._committed_offset = {
            tp: self.committed_offset,
        }

        cthread.tp_last_committed_at = {}
        self._set_last_commit(self.last_commit)

    def test_state(self, *, cthread, now):
        # verify that setup_consumer fixture was applied
        assert cthread.time_started == now


class test_VEP_no_fetch_since_start(test_verify_event_path_base):

    def test_just_started(self, *, cthread, now, tp, logger):
        self._set_started(now - 2.0)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()

    def test_timed_out(self, *, cthread, now, tp, logger):
        self._set_started(
            now - cthread.tp_fetch_request_timeout_secs * 2,
        )
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_called_with(
            mod.SLOW_PROCESSING_NO_FETCH_SINCE_START,
            ANY, ANY,
        )


class test_VEP_no_response_since_start(test_verify_event_path_base):

    def test_just_started(self, *, cthread, _consumer, now, tp, logger):
        self._set_last_request(now - 5.0)
        self._set_started(now - 2.0)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()

    def test_timed_out(self, *, cthread, _consumer, now, tp, logger):
        assert cthread.verify_event_path(now, tp) is None
        self._set_last_request(now - 5.0)
        self._set_started(
            now - cthread.tp_fetch_response_timeout_secs * 2,
        )
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_called_with(
            mod.SLOW_PROCESSING_NO_RESPONSE_SINCE_START,
            ANY, ANY,
        )


class test_VEP_no_recent_fetch(test_verify_event_path_base):

    def test_recent_fetch(self, *, cthread, now, tp, logger):
        self._set_last_response(now - 30.0)
        self._set_last_request(now - 2.0)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()

    def test_timed_out(self, *, cthread, now, tp, logger):
        self._set_last_response(now - 30.0)
        self._set_last_request(now - cthread.tp_fetch_request_timeout_secs * 2)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_called_with(
            mod.SLOW_PROCESSING_NO_RECENT_FETCH,
            ANY, ANY,
        )


class test_VEP_no_recent_response(test_verify_event_path_base):

    def test_recent_response(self, *, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 2.0)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()

    def test_timed_out(self, *, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(
            now - cthread.tp_fetch_response_timeout_secs * 2)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_called_with(
            mod.SLOW_PROCESSING_NO_RECENT_RESPONSE,
            ANY, ANY,
        )


class test_VEP_no_highwater_since_start(test_verify_event_path_base):
    highwater = None

    def test_no_monitor(self, *, app, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 5.0)
        self._set_started(now)
        app.monitor = None
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()

    def test_just_started(self, *, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 5.0)
        self._set_started(now)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()

    def test_timed_out(self, *, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 5.0)
        self._set_started(now - cthread.tp_stream_timeout_secs * 2)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_called_with(
            mod.SLOW_PROCESSING_NO_HIGHWATER_SINCE_START,
            ANY, ANY,
        )


class test_VEP_stream_idle_no_highwater(test_verify_event_path_base):

    highwater = 10
    committed_offset = 10

    def test_highwater_same_as_offset(self, *, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 5.0)
        self._set_started(now - 300.0)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()


class test_VEP_stream_idle_highwater_no_acks(
        test_verify_event_path_base):
    acks_enabled = False

    def test_no_acks(self, *, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 5.0)
        self._set_started(now)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()


class test_VEP_stream_idle_highwater_same_has_acks_everything_OK(
        test_verify_event_path_base):
    highwater = 10
    committed_offset = 10
    inbound_time = None
    acks_enabled = True

    def test_main(self, *, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 5.0)
        self._set_started(now)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()


class test_VEP_stream_idle_highwater_no_inbound(
        test_verify_event_path_base):
    highwater = 20
    committed_offset = 10
    inbound_time = None
    acks_enabled = True

    def test_just_started(self, *, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 5.0)
        self._set_started(now)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()

    def test_timed_out_since_start(self, *, app, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 5.0)
        self._set_started(now - cthread.tp_stream_timeout_secs * 2)
        assert cthread.verify_event_path(now, tp) is None
        expected_message = cthread._make_slow_processing_error(
            mod.SLOW_PROCESSING_STREAM_IDLE_SINCE_START,
            [mod.SLOW_PROCESSING_CAUSE_STREAM,
             mod.SLOW_PROCESSING_CAUSE_AGENT])
        logger.error.assert_called_once_with(
            expected_message,
            tp, ANY,
            setting='stream_processing_timeout',
            current_value=app.conf.stream_processing_timeout,
        )

    def test_has_inbound(self, *, app, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 5.0)
        self._set_started(now - cthread.tp_stream_timeout_secs * 2)
        self._set_stream_inbound(now)
        self._set_last_commit(now)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()

    def test_inbound_timed_out(self, *, app, cthread, now, tp, logger):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 5.0)
        self._set_started(now - cthread.tp_stream_timeout_secs * 4)
        self._set_stream_inbound(now - cthread.tp_stream_timeout_secs * 2)
        self._set_last_commit(now)
        assert cthread.verify_event_path(now, tp) is None
        expected_message = cthread._make_slow_processing_error(
            mod.SLOW_PROCESSING_STREAM_IDLE,
            [mod.SLOW_PROCESSING_CAUSE_STREAM,
             mod.SLOW_PROCESSING_CAUSE_AGENT])
        logger.error.assert_called_once_with(
            expected_message,
            tp, ANY,
            setting='stream_processing_timeout',
            current_value=app.conf.stream_processing_timeout,
        )


class test_VEP_no_commit(test_verify_event_path_base):
    highwater = 20
    committed_offset = 10
    inbound_time = None
    acks_enabled = True

    def _configure(self, now, cthread):
        self._set_last_request(now - 10.0)
        self._set_last_response(now - 5.0)
        self._set_started(now - cthread.tp_stream_timeout_secs * 4)
        self._set_stream_inbound(now - 0.01)

    def test_just_started(self, *, cthread, now, tp, logger):
        self._configure(now, cthread)
        self._set_last_commit(None)
        self._set_started(now)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()

    def test_timed_out_since_start(self, *, app, cthread, now, tp, logger):
        self._configure(now, cthread)
        self._set_last_commit(None)
        self._set_started(now - cthread.tp_commit_timeout_secs * 2)
        assert cthread.verify_event_path(now, tp) is None
        expected_message = cthread._make_slow_processing_error(
            mod.SLOW_PROCESSING_NO_COMMIT_SINCE_START,
            [mod.SLOW_PROCESSING_CAUSE_COMMIT],
        )
        logger.error.assert_called_once_with(
            expected_message,
            tp, ANY,
            setting='broker_commit_livelock_soft_timeout',
            current_value=app.conf.broker_commit_livelock_soft_timeout,
        )

    def test_timed_out_since_last(self, *, app, cthread, now, tp, logger):
        self._configure(now, cthread)
        self._set_last_commit(cthread.tp_commit_timeout_secs * 2)
        self._set_started(now - cthread.tp_commit_timeout_secs * 4)
        assert cthread.verify_event_path(now, tp) is None
        expected_message = cthread._make_slow_processing_error(
            mod.SLOW_PROCESSING_NO_RECENT_COMMIT,
            [mod.SLOW_PROCESSING_CAUSE_COMMIT],
        )
        logger.error.assert_called_once_with(
            expected_message,
            tp, ANY,
            setting='broker_commit_livelock_soft_timeout',
            current_value=app.conf.broker_commit_livelock_soft_timeout,
        )

    def test_committing_fine(self, *, app, cthread, now, tp, logger):
        self._configure(now, cthread)
        self._set_last_commit(now - 2.0)
        self._set_started(now - cthread.tp_commit_timeout_secs * 4)
        assert cthread.verify_event_path(now, tp) is None
        logger.error.assert_not_called()


class test_AIOKafkaConsumerThread(AIOKafkaConsumerThreadFixtures):

    def test_constructor(self, *, cthread):
        assert cthread._partitioner
        assert cthread._rebalance_listener

    @pytest.mark.asyncio
    async def test_on_start(self, *, cthread, _consumer):
        cthread._create_consumer = Mock(
            name='_create_consumer',
            return_value=_consumer,
        )
        await cthread.on_start()

        assert cthread._consumer is cthread._create_consumer.return_value
        cthread._create_consumer.assert_called_once_with(
            loop=cthread.thread_loop)
        cthread._consumer.start.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_thread_stop(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        await cthread.on_thread_stop()
        cthread._consumer.stop.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_thread_stop__consumer_not_started(self, *, cthread):
        cthread._consumer = None
        await cthread.on_thread_stop()

    def test__create_consumer__client(self, *, cthread, app):
        app.client_only = True
        loop = Mock(name='loop')
        cthread._create_client_consumer = Mock(name='_create_client_consumer')
        c = cthread._create_consumer(loop=loop)
        assert c is cthread._create_client_consumer.return_value
        cthread._create_client_consumer.assert_called_once_with(
            cthread.transport, loop=loop)

    def test__create_consumer__worker(self, *, cthread, app):
        app.client_only = False
        loop = Mock(name='loop')
        cthread._create_worker_consumer = Mock(name='_create_worker_consumer')
        c = cthread._create_consumer(loop=loop)
        assert c is cthread._create_worker_consumer.return_value
        cthread._create_worker_consumer.assert_called_once_with(
            cthread.transport, loop=loop)

    def test_session_gt_request_timeout(self, *, cthread, app):
        app.conf.broker_session_timeout = 90
        app.conf.broker_request_timeout = 10

        with pytest.raises(ImproperlyConfigured):
            self.assert_create_worker_consumer(
                cthread, app,
                in_transaction=False)

    def test__create_worker_consumer(self, *, cthread, app):
        self.assert_create_worker_consumer(
            cthread, app,
            in_transaction=False,
            isolation_level='read_uncommitted',
        )

    def test__create_worker_consumer__transaction(self, *, cthread, app):
        self.assert_create_worker_consumer(
            cthread, app,
            in_transaction=True,
            isolation_level='read_committed',
        )

    def assert_create_worker_consumer(self, cthread, app,
                                      in_transaction=False,
                                      isolation_level='read_uncommitted',
                                      api_version=None):
        loop = Mock(name='loop')
        transport = cthread.transport
        conf = app.conf
        cthread.consumer.in_transaction = in_transaction
        auth_settings = credentials_to_aiokafka_auth(
            conf.broker_credentials, conf.ssl_context)
        with patch('aiokafka.AIOKafkaConsumer') as AIOKafkaConsumer:
            c = cthread._create_worker_consumer(transport, loop)
            assert c is AIOKafkaConsumer.return_value
            max_poll_interval = conf.broker_max_poll_interval
            AIOKafkaConsumer.assert_called_once_with(
                loop=loop,
                api_version=app.conf.consumer_api_version,
                client_id=conf.broker_client_id,
                group_id=conf.id,
                bootstrap_servers=server_list(
                    transport.url, transport.default_port),
                partition_assignment_strategy=[cthread._assignor],
                enable_auto_commit=False,
                auto_offset_reset=conf.consumer_auto_offset_reset,
                max_poll_records=conf.broker_max_poll_records,
                max_poll_interval_ms=int(max_poll_interval * 1000.0),
                max_partition_fetch_bytes=conf.consumer_max_fetch_size,
                fetch_max_wait_ms=1500,
                request_timeout_ms=int(conf.broker_request_timeout * 1000.0),
                rebalance_timeout_ms=int(
                    conf.broker_rebalance_timeout * 1000.0),
                check_crcs=conf.broker_check_crcs,
                session_timeout_ms=int(conf.broker_session_timeout * 1000.0),
                heartbeat_interval_ms=int(
                    conf.broker_heartbeat_interval * 1000.0),
                isolation_level=isolation_level,
                traced_from_parent_span=cthread.traced_from_parent_span,
                start_rebalancing_span=cthread.start_rebalancing_span,
                start_coordinator_span=cthread.start_coordinator_span,
                on_generation_id_known=cthread.on_generation_id_known,
                flush_spans=cthread.flush_spans,
                **auth_settings,
            )

    def test__create_client_consumer(self, *, cthread, app):
        loop = Mock(name='loop')
        transport = cthread.transport
        conf = app.conf
        auth_settings = credentials_to_aiokafka_auth(
            conf.broker_credentials, conf.ssl_context)
        with patch('aiokafka.AIOKafkaConsumer') as AIOKafkaConsumer:
            c = cthread._create_client_consumer(transport, loop)
            max_poll_interval = conf.broker_max_poll_interval
            assert c is AIOKafkaConsumer.return_value
            AIOKafkaConsumer.assert_called_once_with(
                loop=loop,
                client_id=conf.broker_client_id,
                bootstrap_servers=server_list(
                    transport.url, transport.default_port),
                request_timeout_ms=int(conf.broker_request_timeout * 1000.0),
                max_poll_interval_ms=int(max_poll_interval * 1000.0),
                enable_auto_commit=True,
                max_poll_records=conf.broker_max_poll_records,
                auto_offset_reset=conf.consumer_auto_offset_reset,
                check_crcs=conf.broker_check_crcs,
                **auth_settings,
            )

    def test__start_span(self, *, cthread, app):
        with patch(TESTED_MODULE + '.set_current_span') as s:
            app.tracer = Mock(name='tracer')
            span = cthread._start_span('test')
            app.tracer.get_tracer.assert_called_once_with(
                f'{app.conf.name}-_aiokafka')
            tracer = app.tracer.get_tracer.return_value
            tracer.start_span.assert_called_once_with(
                operation_name='test')
            span.set_tag.assert_has_calls([
                call(tags.SAMPLING_PRIORITY, 1),
                call('faust_app', app.conf.name),
                call('faust_id', app.conf.id),
            ])
            s.assert_called_once_with(span)
            assert span is tracer.start_span.return_value

    def test_trace_category(self, *, cthread, app):
        assert cthread.trace_category == f'{app.conf.name}-_aiokafka'

    def test_transform_span_lazy(self, *, cthread, app, tracer):
        cthread._consumer = Mock(name='_consumer')
        cthread._consumer._coordinator.generation = -1
        self.assert_setup_lazy_spans(cthread, app, tracer)

        cthread._consumer._coordinator.generation = 10
        pending = cthread._pending_rebalancing_spans
        assert len(pending) == 3

        cthread.on_generation_id_known()
        assert not pending

    def test_transform_span_flush_spans(self, *, cthread, app, tracer):
        cthread._consumer = Mock(name='_consumer')
        cthread._consumer._coordinator.generation = -1
        self.assert_setup_lazy_spans(cthread, app, tracer)
        pending = cthread._pending_rebalancing_spans
        assert len(pending) == 3

        cthread.flush_spans()
        assert not pending

    def test_span_without_operation_name(self, *, cthread):
        span = opentracing.Span(
            tracer=Mock('tobj'),
            context=opentracing.SpanContext(),
        )

        assert cthread._on_span_cancelled_early(span) is None

    def test_transform_span_lazy_no_consumer(self, *, cthread, app, tracer):
        cthread._consumer = Mock(name='_consumer')
        cthread._consumer._coordinator.generation = -1
        self.assert_setup_lazy_spans(cthread, app, tracer)

        cthread._consumer = None
        pending = cthread._pending_rebalancing_spans
        assert len(pending) == 3

        while pending:
            span = pending.popleft()
            cthread._on_span_generation_known(span)

    def test_transform_span_eager(self, *, cthread, app, tracer):
        cthread._consumer = Mock(name='_consumer')
        cthread._consumer._coordinator.generation = 10
        self.assert_setup_lazy_spans(cthread, app, tracer, expect_lazy=False)

    def assert_setup_lazy_spans(self, cthread, app, tracer, expect_lazy=True):
        got_foo = got_bar = got_baz = False

        def foo():
            nonlocal got_foo
            got_foo = True
            T = cthread.traced_from_parent_span(None, lazy=True)
            T(bar)()

        def bar():
            nonlocal got_bar
            got_bar = True
            T = cthread.traced_from_parent_span(None, lazy=True)
            T(REPLACE_WITH_MEMBER_ID)()

        def REPLACE_WITH_MEMBER_ID():
            nonlocal got_baz
            got_baz = True

        with cthread.start_rebalancing_span() as span:
            T = cthread.traced_from_parent_span(span)
            T(foo)()
            if expect_lazy:
                assert len(cthread._pending_rebalancing_spans) == 2

        assert got_foo
        assert got_bar
        assert got_baz
        if expect_lazy:
            assert len(cthread._pending_rebalancing_spans) == 3
        else:
            assert not cthread._pending_rebalancing_spans

    def test__start_span__no_tracer(self, *, cthread, app):
        app.tracer = None
        with cthread._start_span('test') as span:
            assert span

    def test_traced_from_parent_span(self, *, cthread):
        with patch(TESTED_MODULE + '.traced_from_parent_span') as traced:
            parent_span = Mock(name='parent_span')
            ret = cthread.traced_from_parent_span(parent_span, foo=303)
            traced.assert_called_once_with(parent_span, callback=None, foo=303)
            assert ret is traced.return_value

    def test_start_rebalancing_span(self, *, cthread):
        cthread._start_span = Mock()
        ret = cthread.start_rebalancing_span()
        assert ret is cthread._start_span.return_value
        cthread._start_span.assert_called_once_with('rebalancing', lazy=True)

    def test_start_coordinator_span(self, *, cthread):
        cthread._start_span = Mock()
        ret = cthread.start_coordinator_span()
        assert ret is cthread._start_span.return_value
        cthread._start_span.assert_called_once_with('coordinator')

    def test_close(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.close()
        _consumer.set_close.assert_called_once_with()
        _consumer._coordinator.set_close.assert_called_once_with()

    def test_close__no_consumer(self, *, cthread):
        cthread._consumer = None
        cthread.close()

    @pytest.mark.asyncio
    async def test_subscribe(self, *, cthread, _consumer):
        with self.assert_calls_thread(
                cthread, _consumer, _consumer.subscribe,
                topics={'foo', 'bar'},
                listener=cthread._rebalance_listener):
            await cthread.subscribe(['foo', 'bar'])

    @pytest.mark.asyncio
    async def test_seek_to_committed(self, *, cthread, _consumer):
        with self.assert_calls_thread(
                cthread, _consumer, _consumer.seek_to_committed):
            await cthread.seek_to_committed()

    @pytest.mark.asyncio
    async def test_commit(self, *, cthread, _consumer):
        offsets = {TP1: 100}
        with self.assert_calls_thread(
                cthread, _consumer, cthread._commit, offsets):
            await cthread.commit(offsets)

    @pytest.mark.asyncio
    async def test__commit(self, *, cthread, _consumer):
        offsets = {TP1: 1001}
        cthread._consumer = _consumer
        await cthread._commit(offsets)

        _consumer.commit.assert_called_once_with(
            {TP1: OffsetAndMetadata(1001, '')},
        )

    @pytest.mark.asyncio
    async def test__commit__already_rebalancing(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        _consumer.commit.side_effect = CommitFailedError('already rebalanced')
        assert not (await cthread._commit({TP1: 1001}))

    @pytest.mark.asyncio
    async def test__commit__CommitFailedError(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        exc = _consumer.commit.side_effect = CommitFailedError('xx')
        cthread.crash = AsyncMock()
        assert not (await cthread._commit({TP1: 1001}))
        cthread.crash.assert_called_once_with(exc)

    @pytest.mark.asyncio
    async def test__commit__IllegalStateError(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.assignment = Mock()
        exc = _consumer.commit.side_effect = IllegalStateError('xx')
        cthread.crash = AsyncMock()
        assert not (await cthread._commit({TP1: 1001}))
        cthread.crash.assert_called_once_with(exc)

    @pytest.mark.asyncio
    async def test_position(self, *, cthread, _consumer):
        with self.assert_calls_thread(
                cthread, _consumer, _consumer.position, TP1):
            await cthread.position(TP1)

    @pytest.mark.asyncio
    async def test_seek_to_beginning(self, *, cthread, _consumer):
        partitions = (TP1,)
        with self.assert_calls_thread(
                cthread, _consumer, _consumer.seek_to_beginning,
                *partitions):
            await cthread.seek_to_beginning(*partitions)

    @pytest.mark.asyncio
    async def test_seek_wait(self, *, cthread, _consumer):
        partitions = {TP1: 1001}
        with self.assert_calls_thread(
                cthread, _consumer, cthread._seek_wait,
                _consumer, partitions):
            await cthread.seek_wait(partitions)

    @pytest.mark.asyncio
    async def test__seek_wait(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.consumer._read_offset.clear()
        partitions = {TP1: 0, TP2: 3}

        await cthread._seek_wait(_consumer, partitions)

        assert cthread.consumer._read_offset[TP2] == 3
        assert TP1 not in cthread.consumer._read_offset

        _consumer.position.assert_has_calls([
            call(TP1),
            call(TP2),
        ])

    @pytest.mark.asyncio
    async def test__seek_wait__empty(self, *, cthread, _consumer):
        await cthread._seek_wait(_consumer, {})

    def test_seek(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.seek(TP1, 10)
        _consumer.seek.assert_called_once_with(TP1, 10)

    def test_assignment(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        _consumer.assignment.return_value = {
            TopicPartition(TP1.topic, TP1.partition),
        }
        assignment = cthread.assignment()
        assert assignment == {TP1}
        assert all(isinstance(x, TP) for x in assignment)

    def test_highwater(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.consumer.in_transaction = False
        ret = cthread.highwater(TP1)
        assert ret is _consumer.highwater.return_value
        _consumer.highwater.assert_called_once_with(TP1)

    def test_highwater__in_transaction(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.consumer.in_transaction = True
        ret = cthread.highwater(TP1)
        assert ret is _consumer.last_stable_offset.return_value
        _consumer.last_stable_offset.assert_called_once_with(TP1)

    def test_topic_partitions(self, *, cthread, _consumer):
        cthread._consumer = None
        assert cthread.topic_partitions('foo') is None
        cthread._consumer = _consumer
        assert cthread.topic_partitions('foo') is (
            _consumer._coordinator._metadata_snapshot.get.return_value)

    @pytest.mark.asyncio
    async def test_earliest_offsets(self, *, cthread, _consumer):
        with self.assert_calls_thread(
                cthread, _consumer, _consumer.beginning_offsets, (TP1,)):
            await cthread.earliest_offsets(TP1)

    @pytest.mark.asyncio
    async def test_highwaters(self, *, cthread, _consumer):
        with self.assert_calls_thread(
                cthread, _consumer, cthread._highwaters, (TP1,)):
            await cthread.highwaters(TP1)

    @pytest.mark.asyncio
    async def test__highwaters(self, *, cthread, _consumer):
        cthread.consumer.in_transaction = False
        cthread._consumer = _consumer
        assert await cthread._highwaters([TP1]) is (
            _consumer.end_offsets.coro.return_value)

    @pytest.mark.asyncio
    async def test__highwaters__in_transaction(self, *, cthread, _consumer):
        cthread.consumer.in_transaction = True
        cthread._consumer = _consumer
        assert await cthread._highwaters([TP1]) == {
            TP1: _consumer.last_stable_offset.return_value,
        }

    def test__ensure_consumer(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        assert cthread._ensure_consumer() is _consumer
        cthread._consumer = None
        with pytest.raises(ConsumerNotStarted):
            cthread._ensure_consumer()

    @pytest.mark.asyncio
    async def test_getmany(self, *, cthread, _consumer):
        timeout = 13.1
        active_partitions = {TP1}
        with self.assert_calls_thread(
                cthread, _consumer, cthread._fetch_records,
                _consumer,
                active_partitions,
                timeout=timeout,
                max_records=_consumer._max_poll_records):
            await cthread.getmany(active_partitions, timeout)

    @pytest.mark.asyncio
    async def test__fetch_records__flow_inactive(self, *, cthread, _consumer):
        cthread.consumer.flow_active = False
        assert await cthread._fetch_records(_consumer, set()) == {}

    @pytest.mark.asyncio
    async def test__fetch_records_consumer_closed(self, *, cthread, _consumer):
        _consumer._closed = True
        with pytest.raises(ConsumerStoppedError):
            await cthread._fetch_records(_consumer, set())

    @pytest.mark.asyncio
    async def test__fetch_records_fetcher_closed(self, *, cthread, _consumer):
        _consumer._closed = False
        _consumer._fetcher._closed = True
        with pytest.raises(ConsumerStoppedError):
            await cthread._fetch_records(_consumer, set())

    @pytest.mark.asyncio
    async def test__fetch_records(self, *, cthread, _consumer):
        _consumer._closed = False
        fetcher = _consumer._fetcher
        fetcher._closed = False
        fetcher._subscriptions.fetch_context = MagicMock()
        fetcher.fetched_records = AsyncMock()
        ret = await cthread._fetch_records(
            _consumer, {TP1}, timeout=312.3, max_records=1000)
        assert ret is fetcher.fetched_records.coro.return_value
        fetcher.fetched_records.assert_called_once_with(
            {TP1},
            timeout=312.3,
            max_records=1000,
        )

    @pytest.mark.asyncio
    async def test_create_topic(self, *, cthread, _consumer):
        topic = 'topic'
        partitions = 30
        replication = 11
        config = {'cfg': 'foo'}
        timeout = 30.312
        retention = 1210320.3
        compacting = True
        deleting = False
        ensure_created = True

        transport = cthread.transport
        with self.assert_calls_thread(
                cthread, _consumer, transport._create_topic,
                cthread,
                _consumer._client,
                topic,
                partitions,
                replication,
                config=config,
                timeout=int(timeout * 1000.0),
                retention=int(retention * 1000.0),
                compacting=compacting,
                deleting=deleting,
                ensure_created=ensure_created):
            await cthread.create_topic(
                topic,
                partitions,
                replication,
                config=config,
                timeout=timeout,
                retention=retention,
                compacting=compacting,
                deleting=deleting,
                ensure_created=ensure_created,
            )

    @pytest.mark.asyncio
    async def test_create_topic_invalid_name(self, cthread, _consumer):
        topic = ''.join(random.choices(
            string.ascii_uppercase, k=TOPIC_LENGTH_MAX + 1))
        cthread._consumer = _consumer

        msg = f'Topic name {topic!r} is too long (max={TOPIC_LENGTH_MAX})'
        with pytest.raises(ValueError):
            await cthread.create_topic(topic, 1, 1)
            pytest.fail(msg)

    def test_key_partition(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread._partitioner = Mock(name='partitioner')
        metadata = _consumer._client.cluster
        metadata.partitions_for_topic.return_value = [1, 2, 3]
        metadata.available_partitions_for_topic.return_value = [2, 3]

        cthread.key_partition('topic', 'k', None)
        cthread._partitioner.assert_called_once_with(
            'k', [1, 2, 3], [2, 3],
        )

        with pytest.raises(AssertionError):
            cthread.key_partition('topic', 'k', -1)
        with pytest.raises(AssertionError):
            cthread.key_partition('topic', 'k', 4)

        assert cthread.key_partition('topic', 'k', 3) == 3

    def test_key_partition__no_metadata(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread._partitioner = Mock(name='partitioner')
        metadata = _consumer._client.cluster
        metadata.partitions_for_topic.return_value = None

        assert cthread.key_partition('topic', 'k', None) is None

    @contextmanager
    def assert_calls_thread(self, cthread, _consumer, method, *args, **kwargs):
        cthread._consumer = _consumer
        cthread.call_thread = AsyncMock()
        try:
            yield
        finally:
            cthread.call_thread.assert_called_once_with(
                method, *args, **kwargs)


class MyPartitioner:
    ...


my_partitioner = MyPartitioner()


class test_Producer:

    @pytest.fixture()
    def producer(self, *, app, _producer):
        producer = Producer(app.transport)
        producer._producer = _producer
        return producer

    @pytest.fixture()
    def _producer(self):
        return Mock(
            name='AIOKafkaProducer',
            autospec=aiokafka.AIOKafkaProducer,
            start=AsyncMock(),
            stop=AsyncMock(),
            begin_transaction=AsyncMock(),
            commit_transaction=AsyncMock(),
            abort_transaction=AsyncMock(),
            stop_transaction=AsyncMock(),
            maybe_begin_transaction=AsyncMock(),
            commit=AsyncMock(),
            send=AsyncMock(),
            flush=AsyncMock(),
        )

    @pytest.mark.conf(producer_partitioner=my_partitioner)
    def test_producer__uses_custom_partitioner(self, *, producer):
        assert producer.partitioner is my_partitioner

    @pytest.mark.asyncio
    async def test_begin_transaction(self, *, producer, _producer):
        await producer.begin_transaction('tid')
        _producer.begin_transaction.assert_called_once_with('tid')

    @pytest.mark.asyncio
    async def test_commit_transaction(self, *, producer, _producer):
        await producer.commit_transaction('tid')
        _producer.commit_transaction.assert_called_once_with('tid')

    @pytest.mark.asyncio
    async def test_abort_transaction(self, *, producer, _producer):
        await producer.abort_transaction('tid')
        _producer.abort_transaction.assert_called_once_with('tid')

    @pytest.mark.asyncio
    async def test_stop_transaction(self, *, producer, _producer):
        await producer.stop_transaction('tid')
        _producer.stop_transaction.assert_called_once_with('tid')

    @pytest.mark.asyncio
    async def test_maybe_begin_transaction(self, *, producer, _producer):
        await producer.maybe_begin_transaction('tid')
        _producer.maybe_begin_transaction.assert_called_once_with('tid')

    @pytest.mark.asyncio
    async def test_commit_transactions(self, *, producer, _producer):
        tid_to_offset_map = {'t1': {TP1: 1001}, 't2': {TP2: 2002}}
        await producer.commit_transactions(
            tid_to_offset_map, 'group_id', start_new_transaction=False)
        _producer.commit.assert_called_once_with(
            tid_to_offset_map, 'group_id', start_new_transaction=False)

    def test__settings_extra(self, *, producer, app):
        app.in_transaction = True
        assert producer._settings_extra() == {'acks': 'all'}
        app.in_transaction = False
        assert producer._settings_extra() == {}

    def test__new_producer(self, *, producer):
        self.assert_new_producer(producer)

    @pytest.mark.parametrize('expected_args', [
        pytest.param({'api_version': '0.10'},
                     marks=pytest.mark.conf(
                         producer_api_version='0.10')),
        pytest.param({'acks': 'all'},
                     marks=pytest.mark.conf(
                         producer_acks='all')),
        pytest.param({'bootstrap_servers': ['a:9092', 'b:9092']},
                     marks=pytest.mark.conf(
                         broker='kafka://a:9092;b:9092')),
        pytest.param({'client_id': 'foo'},
                     marks=pytest.mark.conf(
                         broker_client_id='foo')),
        pytest.param({'compression_type': 'snappy'},
                     marks=pytest.mark.conf(
                         producer_compression_type='snappy')),
        pytest.param({'linger_ms': 9345},
                     marks=pytest.mark.conf(
                         producer_linger_ms=9345)),
        pytest.param({'max_batch_size': 41223},
                     marks=pytest.mark.conf(
                         producer_max_batch_size=41223)),
        pytest.param({'max_request_size': 183831},
                     marks=pytest.mark.conf(
                         producer_max_request_size=183831)),
        pytest.param({'request_timeout_ms': 1234134000},
                     marks=pytest.mark.conf(
                         producer_request_timeout=1234134)),
        pytest.param(
            {'security_protocol': 'SASL_PLAINTEXT',
             'sasl_mechanism': 'PLAIN',
             'sasl_plain_username': 'uname',
             'sasl_plain_password': 'pw',
             'ssl_context': None},
            marks=pytest.mark.conf(
                broker_credentials=auth.SASLCredentials(
                    username='uname',
                    password='pw',
                    mechanism='PLAIN'),
            ),
        ),
    ])
    def test__new_producer__using_settings(self, expected_args, *,
                                           app, producer):
        self.assert_new_producer(producer, **expected_args)

    def assert_new_producer(self, producer,
                            acks=-1,
                            api_version='auto',
                            bootstrap_servers=['localhost:9092'],  # noqa,
                            client_id=f'faust-{faust.__version__}',
                            compression_type=None,
                            linger_ms=0,
                            max_batch_size=16384,
                            max_request_size=1000000,
                            request_timeout_ms=1200000,
                            security_protocol='PLAINTEXT',
                            **kwargs):
        with patch('aiokafka.AIOKafkaProducer') as AIOKafkaProducer:
            p = producer._new_producer()
            assert p is AIOKafkaProducer.return_value
            AIOKafkaProducer.assert_called_once_with(
                acks=acks,
                api_version=api_version,
                bootstrap_servers=bootstrap_servers,
                client_id=client_id,
                compression_type=compression_type,
                linger_ms=linger_ms,
                max_batch_size=max_batch_size,
                max_request_size=max_request_size,
                request_timeout_ms=request_timeout_ms,
                security_protocol=security_protocol,
                loop=producer.loop,
                partitioner=producer.partitioner,
                on_irrecoverable_error=producer._on_irrecoverable_error,
                **kwargs,
            )

    def test__new_producer__default(self, *, producer):
        p = producer._new_producer()
        assert isinstance(p, aiokafka.AIOKafkaProducer)

    def test__new_producer__in_transaction(self, *, producer):
        producer.app.in_transaction = True
        p = producer._new_producer()
        assert isinstance(p, aiokafka.MultiTXNProducer)

    def test__producer_type(self, *, producer, app):
        app.in_transaction = True
        assert producer._producer_type is aiokafka.MultiTXNProducer
        app.in_transaction = False
        assert producer._producer_type is aiokafka.AIOKafkaProducer

    @pytest.mark.asyncio
    async def test__on_irrecoverable_error(self, *, producer):
        exc = KeyError()
        producer.crash = AsyncMock()
        app = producer.transport.app
        app.consumer = None
        await producer._on_irrecoverable_error(exc)
        producer.crash.assert_called_once_with(exc)
        app.consumer = Mock(name='consumer')
        app.consumer.crash = AsyncMock()
        await producer._on_irrecoverable_error(exc)
        app.consumer.crash.assert_called_once_with(exc)

    @pytest.mark.asyncio
    async def test_create_topic(self, *, producer, _producer):
        _producer.client = Mock(
            force_metadata_update=AsyncMock(),
        )
        producer.transport = Mock(
            _create_topic=AsyncMock(),
        )
        await producer.create_topic(
            'foo', 100, 3,
            config={'x': 'y'},
            timeout=30.3,
            retention=300.3,
            compacting=True,
            deleting=True,
            ensure_created=True,
        )
        producer.transport._create_topic.coro.assert_called_once_with(
            producer,
            _producer.client,
            'foo',
            100,
            3,
            config={'x': 'y'},
            timeout=int(30.3 * 1000.0),
            retention=int(300.3 * 1000.0),
            compacting=True,
            deleting=True,
            ensure_created=True,
        )

    def test__ensure_producer(self, *, producer, _producer):
        assert producer._ensure_producer() is _producer
        producer._producer = None
        with pytest.raises(NotReady):
            producer._ensure_producer()

    @pytest.mark.asyncio
    async def test_on_start(self, *, producer, loop):
        producer._new_producer = Mock(
            name='_new_producer',
            return_value=Mock(
                start=AsyncMock(),
            ),
        )
        _producer = producer._new_producer.return_value
        producer.beacon = Mock()

        await producer.on_start()
        assert producer._producer is _producer
        producer._new_producer.assert_called_once_with()
        producer.beacon.add.assert_called_with(_producer)
        _producer.start.coro.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_stop(self, *, producer, _producer):
        await producer.on_stop()
        assert producer._producer is None
        _producer.stop.assert_called_once_with()

    def test_supports_headers__not_ready(self, *, producer):
        producer._producer.client = None
        with pytest.raises(NotReady):
            producer.supports_headers()

    @pytest.mark.asyncio
    async def test_send(self, producer, _producer):
        await producer.send(
            'topic', 'k', 'v', 3, 100, {'foo': 'bar'},
            transactional_id='tid',
        )
        _producer.send.assert_called_once_with(
            'topic', 'v',
            key='k',
            partition=3,
            timestamp_ms=100 * 1000.0,
            headers=[('foo', 'bar')],
            transactional_id='tid',
        )

    @pytest.mark.asyncio
    @pytest.mark.conf(producer_api_version='0.10')
    async def test_send__request_no_headers(self, producer, _producer):
        await producer.send(
            'topic', 'k', 'v', 3, 100, {'foo': 'bar'},
            transactional_id='tid',
        )
        _producer.send.assert_called_once_with(
            'topic', 'v',
            key='k',
            partition=3,
            timestamp_ms=100 * 1000.0,
            headers=None,
            transactional_id='tid',
        )

    @pytest.mark.asyncio
    @pytest.mark.conf(producer_api_version='0.11')
    async def test_send__kafka011_supports_headers(self, producer, _producer):
        await producer.send(
            'topic', 'k', 'v', 3, 100, {'foo': 'bar'},
            transactional_id='tid',
        )
        _producer.send.assert_called_once_with(
            'topic', 'v',
            key='k',
            partition=3,
            timestamp_ms=100 * 1000.0,
            headers=[('foo', 'bar')],
            transactional_id='tid',
        )

    @pytest.mark.asyncio
    @pytest.mark.conf(producer_api_version='auto')
    async def test_send__auto_passes_headers(self, producer, _producer):
        await producer.send(
            'topic', 'k', 'v', 3, 100, [('foo', 'bar')],
            transactional_id='tid',
        )
        _producer.send.assert_called_once_with(
            'topic', 'v',
            key='k',
            partition=3,
            timestamp_ms=100 * 1000.0,
            headers=[('foo', 'bar')],
            transactional_id='tid',
        )

    @pytest.mark.asyncio
    async def test_send__no_headers(self, producer, _producer):
        await producer.send(
            'topic', 'k', 'v', 3, 100, None,
            transactional_id='tid',
        )
        _producer.send.assert_called_once_with(
            'topic', 'v',
            key='k',
            partition=3,
            timestamp_ms=100 * 1000.0,
            headers=None,
            transactional_id='tid',
        )

    @pytest.mark.asyncio
    async def test_send__no_timestamp(self, producer, _producer):
        await producer.send(
            'topic', 'k', 'v', 3, None, None,
            transactional_id='tid',
        )
        _producer.send.assert_called_once_with(
            'topic', 'v',
            key='k',
            partition=3,
            timestamp_ms=None,
            headers=None,
            transactional_id='tid',
        )

    @pytest.mark.asyncio
    async def test_send__KafkaError(self, producer, _producer):
        _producer.send.coro.side_effect = KafkaError()
        with pytest.raises(ProducerSendError):
            await producer.send(
                'topic', 'k', 'v', 3, None, None,
                transactional_id='tid',
            )

    @pytest.mark.asyncio
    async def test_send_and_wait(self, producer):
        producer.send = AsyncMock(return_value=done_future(done_future()))

        await producer.send_and_wait(
            'topic', 'k', 'v', 3, 100, [('a', 'b')],
            transactional_id='tid')
        producer.send.assert_called_once_with(
            'topic',
            key='k',
            value='v',
            partition=3,
            timestamp=100,
            headers=[('a', 'b')],
            transactional_id='tid',
        )

    @pytest.mark.asyncio
    async def test_flush(self, *, producer, _producer):
        producer._producer = None
        await producer.flush()
        producer._producer = _producer
        await producer.flush()
        _producer.flush.assert_called_once_with()

    def test_key_partition(self, *, producer, _producer):
        x = producer.key_partition('topic', 'k')
        assert x == TP('topic', _producer._partition.return_value)

    def test_supports_headers(self, *, producer):
        producer._producer.client.api_version = (0, 11)
        assert producer.supports_headers()


class test_Transport:

    @pytest.fixture()
    def transport(self, *, app):
        return Transport(url=['aiokafka://'], app=app)

    def test_constructor(self, *, transport):
        assert transport._topic_waiters == {}

    def test__topic_config(self, *, transport):
        assert transport._topic_config() == {}

    def test__topic_config__retention(self, *, transport):
        assert transport._topic_config(retention=3000.3) == {
            'retention.ms': 3000.3,
        }

    def test__topic_config__compacting(self, *, transport):
        assert transport._topic_config(compacting=True) == {
            'cleanup.policy': 'compact',
        }

    def test__topic_config__deleting(self, *, transport):
        assert transport._topic_config(deleting=True) == {
            'cleanup.policy': 'delete',
        }

    def test__topic_config__combined(self, *, transport):
        res = transport._topic_config(
            compacting=True, deleting=True, retention=3000.3)
        assert res == {
            'retention.ms': 3000.3,
            'cleanup.policy': 'compact,delete',
        }

    @pytest.mark.asyncio
    async def test__create_topic(self, *, transport):
        client = Mock(name='client')
        transport._topic_waiters['foo'] = AsyncMock()
        await transport._create_topic(
            transport,
            client,
            topic='foo',
            partitions=100,
            replication=3,
        )
        transport._topic_waiters['foo'].coro.assert_called_with()

    @pytest.mark.asyncio
    async def test__create_topic__missing(self, *, transport, loop):
        client = Mock(name='client')
        transport._topic_waiters.clear()
        with patch('faust.transport.drivers.aiokafka.StampedeWrapper') as SW:
            SW.return_value = AsyncMock()
            await transport._create_topic(
                transport,
                client,
                topic='foo',
                partitions=100,
                replication=3,
            )
            SW.assert_called_once_with(
                transport._really_create_topic,
                transport,
                client,
                'foo',
                100,
                3,
                loop=loop,
            )
            SW.return_value.coro.assert_called_once_with()
            assert transport._topic_waiters['foo'] is SW.return_value

    @pytest.mark.asyncio
    async def test__create_topic__raises(self, *, transport, loop):
        client = Mock(name='client')
        transport._topic_waiters.clear()
        with patch('faust.transport.drivers.aiokafka.StampedeWrapper') as SW:
            SW.return_value = AsyncMock()
            SW.return_value.coro.side_effect = KeyError('foo')
            with pytest.raises(KeyError):
                await transport._create_topic(
                    transport,
                    client,
                    topic='foo',
                    partitions=100,
                    replication=3,
                )
            assert 'foo' not in transport._topic_waiters


@pytest.mark.parametrize('credentials,ssl_context,expected', [
    (None, {}, {
        'security_protocol': 'SSL', 'ssl_context': {},
    }),
    (None, None, {
        'security_protocol': 'PLAINTEXT',
    }),
    (auth.SSLCredentials({}), None, {
        'security_protocol': 'SSL', 'ssl_context': {},
    }),
    (auth.SASLCredentials(username='foo', password='bar'), None, {
        'security_protocol': 'SASL_PLAINTEXT',
        'sasl_mechanism': 'PLAIN',
        'sasl_plain_username': 'foo',
        'sasl_plain_password': 'bar',
        'ssl_context': None,
    }),
    (auth.GSSAPICredentials(kerberos_service_name='service',
                            kerberos_domain_name='moo'), None, {
        'security_protocol': 'SASL_PLAINTEXT',
        'sasl_mechanism': 'GSSAPI',
        'sasl_kerberos_service_name': 'service',
        'sasl_kerberos_domain_name': 'moo',
        'ssl_context': None,
    }),
])
def test_credentials_to_aiokafka(credentials, ssl_context, expected):
    assert credentials_to_aiokafka_auth(credentials, ssl_context) == expected


def test_credentials_to_aiokafka__invalid():
    with pytest.raises(ImproperlyConfigured):
        credentials_to_aiokafka_auth(object())
