"""Message transport using :pypi:`aiokafka`."""
import asyncio
import typing

from collections import deque
from time import monotonic
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
    no_type_check,
)

import aiokafka
import aiokafka.abc
import opentracing
from aiokafka.consumer.group_coordinator import OffsetCommitRequest
from aiokafka.errors import (
    CommitFailedError,
    ConsumerStoppedError,
    IllegalStateError,
    KafkaError,
)
from aiokafka.structs import (
    OffsetAndMetadata,
    TopicPartition as _TopicPartition,
)
from aiokafka.util import parse_kafka_version
from kafka.errors import (
    NotControllerError,
    TopicAlreadyExistsError as TopicExistsError,
    for_code,
)
from kafka.partitioner.default import DefaultPartitioner
from kafka.partitioner.hashed import murmur2
from kafka.protocol.metadata import MetadataRequest_v1
from mode import Service, get_logger
from mode.utils.futures import StampedeWrapper
from mode.utils.objects import cached_property
from mode.utils import text
from mode.utils.times import Seconds, humanize_seconds_ago, want_seconds
from mode.utils.typing import Deque
from opentracing.ext import tags
from yarl import URL

from faust.auth import (
    GSSAPICredentials,
    SASLCredentials,
    SSLCredentials,
)
from faust.exceptions import (
    ConsumerNotStarted,
    ImproperlyConfigured,
    NotReady,
    ProducerSendError,
)
from faust.transport import base
from faust.transport.consumer import (
    ConsumerThread,
    RecordMap,
    ThreadDelegateConsumer,
    ensure_TPset,
)
from faust.types import ConsumerMessage, HeadersArg, RecordMetadata, TP
from faust.types.auth import CredentialsT
from faust.types.transports import (
    ConsumerT,
    PartitionerT,
    ProducerT,
)
from faust.utils.kafka.protocol.admin import CreateTopicsRequest
from faust.utils.tracing import (
    noop_span,
    set_current_span,
    traced_from_parent_span,
)

__all__ = ['Consumer', 'Producer', 'Transport']

if not hasattr(aiokafka, '__robinhood__'):  # pragma: no cover
    raise RuntimeError(
        'Please install robinhood-aiokafka, not aiokafka')

logger = get_logger(__name__)

DEFAULT_GENERATION_ID = OffsetCommitRequest.DEFAULT_GENERATION_ID

TOPIC_LENGTH_MAX = 249

SLOW_PROCESSING_CAUSE_AGENT = '''
The agent processing the stream is hanging (waiting for network, I/O or \
infinite loop).
'''.strip()

SLOW_PROCESSING_CAUSE_STREAM = '''
The stream has stopped processing events for some reason.
'''.strip()


SLOW_PROCESSING_CAUSE_COMMIT = '''
The commit handler background thread has stopped working (report as bug).
'''.strip()


SLOW_PROCESSING_EXPLAINED = '''

There are multiple possible explanations for this:

1) The processing of a single event in the stream
   is taking too long.

    The timeout for this is defined by the %(setting)s setting,
    currently set to %(current_value)r.  If you expect the time
    required to process an event, to be greater than this then please
    increase the timeout.

'''

SLOW_PROCESSING_NO_FETCH_SINCE_START = '''
Aiokafka has not sent fetch request for %r since start (started %s)
'''.strip()

SLOW_PROCESSING_NO_RESPONSE_SINCE_START = '''
Aiokafka has not received fetch response for %r since start (started %s)
'''.strip()

SLOW_PROCESSING_NO_RECENT_FETCH = '''
Aiokafka stopped fetching from %r (last done %s)
'''.strip()

SLOW_PROCESSING_NO_RECENT_RESPONSE = '''
Broker stopped responding to fetch requests for %r (last responded %s)
'''.strip()

SLOW_PROCESSING_NO_HIGHWATER_SINCE_START = '''
Highwater not yet available for %r (started %s).
'''.strip()

SLOW_PROCESSING_STREAM_IDLE_SINCE_START = '''
Stream has not started processing %r (started %s).
'''.strip()

SLOW_PROCESSING_STREAM_IDLE = '''
Stream stopped processing, or is slow for %r (last inbound %s).
'''.strip()

SLOW_PROCESSING_NO_COMMIT_SINCE_START = '''
Has not committed %r at all since worker start (started %s).
'''.strip()

SLOW_PROCESSING_NO_RECENT_COMMIT = '''
Has not committed %r (last commit %s).
'''.strip()


def server_list(urls: List[URL], default_port: int) -> List[str]:
    """Convert list of urls to list of servers accepted by :pypi:`aiokafka`."""
    default_host = '127.0.0.1'
    return [f'{u.host or default_host}:{u.port or default_port}' for u in urls]


class ConsumerRebalanceListener(
        aiokafka.abc.ConsumerRebalanceListener):  # type: ignore
    # kafka's ridiculous class based callback interface makes this hacky.

    def __init__(self, thread: ConsumerThread) -> None:
        self._thread: ConsumerThread = thread

    def on_partitions_revoked(
            self, revoked: Iterable[_TopicPartition]) -> Awaitable:
        """Call when partitions are being revoked."""
        thread = self._thread
        # XXX Must call app.on_rebalance_start as early as possible.
        # we call this in the sync method, this way when we know
        # that it will be called even if await never returns to the coroutine.
        thread.app.on_rebalance_start()

        # this way we should also get a warning if the coroutine
        # is never awaited.
        return thread.on_partitions_revoked(ensure_TPset(revoked))

    async def on_partitions_assigned(
            self, assigned: Iterable[_TopicPartition]) -> None:
        """Call when partitions are being assigned."""
        await self._thread.on_partitions_assigned(ensure_TPset(assigned))


class Consumer(ThreadDelegateConsumer):
    """Kafka consumer using :pypi:`aiokafka`."""

    logger = logger

    RebalanceListener: ClassVar[Type[ConsumerRebalanceListener]]
    RebalanceListener = ConsumerRebalanceListener

    consumer_stopped_errors: ClassVar[Tuple[Type[BaseException], ...]] = (
        ConsumerStoppedError,
    )

    def _new_consumer_thread(self) -> ConsumerThread:
        return AIOKafkaConsumerThread(self, loop=self.loop, beacon=self.beacon)

    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 30.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        """Create/declare topic on server."""
        await self._thread.create_topic(
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

    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        return cast(TP, _TopicPartition(topic, partition))

    def _to_message(self, tp: TP, record: Any) -> ConsumerMessage:
        timestamp: Optional[int] = record.timestamp
        timestamp_s: float = cast(float, None)
        if timestamp is not None:
            timestamp_s = timestamp / 1000.0
        return ConsumerMessage(
            record.topic,
            record.partition,
            record.offset,
            timestamp_s,
            record.timestamp_type,
            record.headers,
            record.key,
            record.value,
            record.checksum,
            record.serialized_key_size,
            record.serialized_value_size,
            tp,
        )

    async def on_stop(self) -> None:
        """Call when consumer is stopping."""
        await super().on_stop()
        transport = cast(Transport, self.transport)
        transport._topic_waiters.clear()


class AIOKafkaConsumerThread(ConsumerThread):
    _consumer: Optional[aiokafka.AIOKafkaConsumer] = None
    _pending_rebalancing_spans: Deque[opentracing.Span]

    tp_last_committed_at: MutableMapping[TP, float]
    time_started: float

    tp_fetch_request_timeout_secs: float
    tp_fetch_response_timeout_secs: float
    tp_stream_timeout_secs: float
    tp_commit_timeout_secs: float

    def __post_init__(self) -> None:
        consumer = cast(Consumer, self.consumer)
        self._partitioner: PartitionerT = (
            self.app.conf.producer_partitioner or DefaultPartitioner())
        self._rebalance_listener = consumer.RebalanceListener(self)
        self._pending_rebalancing_spans = deque()
        self.tp_last_committed_at = {}

        app = self.consumer.app

        stream_processing_timeout = app.conf.stream_processing_timeout
        self.tp_fetch_request_timeout_secs = stream_processing_timeout
        self.tp_fetch_response_timeout_secs = stream_processing_timeout
        self.tp_stream_timeout_secs = stream_processing_timeout

        commit_livelock_timeout = app.conf.broker_commit_livelock_soft_timeout
        self.tp_commit_timeout_secs = commit_livelock_timeout

    async def on_start(self) -> None:
        """Call when consumer starts."""
        self._consumer = self._create_consumer(loop=self.thread_loop)
        self.time_started = monotonic()
        await self._consumer.start()

    async def on_thread_stop(self) -> None:
        """Call when consumer thread is stopping."""
        # super stops thread method queue (QueueServiceThread.method_queue)
        await super().on_thread_stop()
        # when method queue is stopped, we can stop the consumer
        if self._consumer is not None:
            await self._consumer.stop()

    def _create_consumer(
            self,
            loop: asyncio.AbstractEventLoop) -> aiokafka.AIOKafkaConsumer:
        transport = cast(Transport, self.transport)
        if self.app.client_only:
            return self._create_client_consumer(transport, loop=loop)
        else:
            return self._create_worker_consumer(transport, loop=loop)

    def _create_worker_consumer(
            self,
            transport: 'Transport',
            loop: asyncio.AbstractEventLoop) -> aiokafka.AIOKafkaConsumer:
        isolation_level: str = 'read_uncommitted'
        conf = self.app.conf
        if self.consumer.in_transaction:
            isolation_level = 'read_committed'
        self._assignor = self.app.assignor
        auth_settings = credentials_to_aiokafka_auth(
            conf.broker_credentials, conf.ssl_context)
        max_poll_interval = conf.broker_max_poll_interval or 0

        request_timeout = conf.broker_request_timeout
        session_timeout = conf.broker_session_timeout
        rebalance_timeout = conf.broker_rebalance_timeout

        if session_timeout > request_timeout:
            raise ImproperlyConfigured(
                f'Setting broker_session_timeout={session_timeout} '
                f'cannot be greater than '
                f'broker_request_timeout={request_timeout}')

        return aiokafka.AIOKafkaConsumer(
            loop=loop,
            api_version=conf.consumer_api_version,
            client_id=conf.broker_client_id,
            group_id=conf.id,
            bootstrap_servers=server_list(
                transport.url, transport.default_port),
            partition_assignment_strategy=[self._assignor],
            enable_auto_commit=False,
            auto_offset_reset=conf.consumer_auto_offset_reset,
            max_poll_records=conf.broker_max_poll_records,
            max_poll_interval_ms=int(max_poll_interval * 1000.0),
            max_partition_fetch_bytes=conf.consumer_max_fetch_size,
            fetch_max_wait_ms=1500,
            request_timeout_ms=int(request_timeout * 1000.0),
            check_crcs=conf.broker_check_crcs,
            session_timeout_ms=int(session_timeout * 1000.0),
            rebalance_timeout_ms=int(rebalance_timeout * 1000.0),
            heartbeat_interval_ms=int(conf.broker_heartbeat_interval * 1000.0),
            isolation_level=isolation_level,
            traced_from_parent_span=self.traced_from_parent_span,
            start_rebalancing_span=self.start_rebalancing_span,
            start_coordinator_span=self.start_coordinator_span,
            on_generation_id_known=self.on_generation_id_known,
            flush_spans=self.flush_spans,
            **auth_settings,
        )

    def _create_client_consumer(
            self,
            transport: 'Transport',
            loop: asyncio.AbstractEventLoop) -> aiokafka.AIOKafkaConsumer:
        conf = self.app.conf
        auth_settings = credentials_to_aiokafka_auth(
            conf.broker_credentials, conf.ssl_context)
        max_poll_interval = conf.broker_max_poll_interval or 0
        return aiokafka.AIOKafkaConsumer(
            loop=loop,
            client_id=conf.broker_client_id,
            bootstrap_servers=server_list(
                transport.url, transport.default_port),
            request_timeout_ms=int(conf.broker_request_timeout * 1000.0),
            enable_auto_commit=True,
            max_poll_records=conf.broker_max_poll_records,
            max_poll_interval_ms=int(max_poll_interval * 1000.0),
            auto_offset_reset=conf.consumer_auto_offset_reset,
            check_crcs=conf.broker_check_crcs,
            **auth_settings,
        )

    @cached_property
    def trace_category(self) -> str:
        return f'{self.app.conf.name}-_aiokafka'

    def start_rebalancing_span(self) -> opentracing.Span:
        return self._start_span('rebalancing', lazy=True)

    def start_coordinator_span(self) -> opentracing.Span:
        return self._start_span('coordinator')

    def _start_span(self, name: str, *,
                    lazy: bool = False) -> opentracing.Span:
        tracer = self.app.tracer
        if tracer is not None:
            span = tracer.get_tracer(self.trace_category).start_span(
                operation_name=name,
            )
            span.set_tag(tags.SAMPLING_PRIORITY, 1)
            self.app._span_add_default_tags(span)
            set_current_span(span)
            if lazy:
                self._transform_span_lazy(span)
            return span
        else:
            return noop_span()

    @no_type_check
    def _transform_span_lazy(self, span: opentracing.Span) -> None:
        # XXX slow
        consumer = self
        if typing.TYPE_CHECKING:
            # MyPy completely disallows the statements below
            # claiming it is an illegal dynamic baseclass.
            # We know mypy, but do it anyway :D
            pass
        else:
            cls = span.__class__
            class LazySpan(cls):

                def finish() -> None:
                    consumer._span_finish(span)

            span._real_finish, span.finish = span.finish, LazySpan.finish

    def _span_finish(self, span: opentracing.Span) -> None:
        assert self._consumer is not None
        if self._consumer._coordinator.generation == DEFAULT_GENERATION_ID:
            self._on_span_generation_pending(span)
        else:
            self._on_span_generation_known(span)

    def _on_span_generation_pending(self, span: opentracing.Span) -> None:
        self._pending_rebalancing_spans.append(span)

    def _on_span_generation_known(self, span: opentracing.Span) -> None:
        if self._consumer:
            coordinator = self._consumer._coordinator
            coordinator_id = coordinator.coordinator_id
            app_id = self.app.conf.id
            generation = coordinator.generation
            member_id = coordinator.member_id

            try:
                op_name = span.operation_name
                set_tag = span.set_tag
            except AttributeError:  # pragma: no cover
                pass  # not a real span
            else:
                trace_id_str = f'reb-{app_id}-{generation}'
                trace_id = murmur2(trace_id_str.encode())

                span.context.trace_id = trace_id
                if op_name.endswith('.REPLACE_WITH_MEMBER_ID'):
                    span.set_operation_name(f'rebalancing node {member_id}')
                set_tag('kafka_generation', generation)
                set_tag('kafka_member_id', member_id)
                set_tag('kafka_coordinator_id', coordinator_id)
                self.app._span_add_default_tags(span)
                span._real_finish()

    def _on_span_cancelled_early(self, span: opentracing.Span) -> None:
        try:
            op_name = span.operation_name
        except AttributeError:
            return
        else:
            span.set_operation_name(f'{op_name} (CANCELLED)')
            span._real_finish()

    def traced_from_parent_span(self,
                                parent_span: opentracing.Span,
                                lazy: bool = False,
                                **extra_context: Any) -> Callable:
        return traced_from_parent_span(
            parent_span,
            callback=self._transform_span_lazy if lazy else None,
            **extra_context)

    def flush_spans(self) -> None:
        while self._pending_rebalancing_spans:
            span = self._pending_rebalancing_spans.popleft()
            self._on_span_cancelled_early(span)

    def on_generation_id_known(self) -> None:
        while self._pending_rebalancing_spans:
            span = self._pending_rebalancing_spans.popleft()
            self._on_span_generation_known(span)

    def close(self) -> None:
        """Close consumer for graceful shutdown."""
        if self._consumer is not None:
            self._consumer.set_close()
            self._consumer._coordinator.set_close()

    async def subscribe(self, topics: Iterable[str]) -> None:
        """Reset subscription (requires rebalance)."""
        # XXX pattern does not work :/
        await self.call_thread(
            self._ensure_consumer().subscribe,
            topics=set(topics),
            listener=self._rebalance_listener,
        )

    async def seek_to_committed(self) -> Mapping[TP, int]:
        """Seek partitions to the last committed offset."""
        return await self.call_thread(
            self._ensure_consumer().seek_to_committed)

    async def commit(self, offsets: Mapping[TP, int]) -> bool:
        """Commit topic offsets."""
        return await self.call_thread(self._commit, offsets)

    async def _commit(self, offsets: Mapping[TP, int]) -> bool:
        consumer = self._ensure_consumer()
        now = monotonic()
        try:
            aiokafka_offsets = {
                tp: OffsetAndMetadata(offset, '')
                for tp, offset in offsets.items()
            }
            self.tp_last_committed_at.update({
                tp: now
                for tp in offsets
            })
            await consumer.commit(aiokafka_offsets)
        except CommitFailedError as exc:
            if 'already rebalanced' in str(exc):
                return False
            self.log.exception(f'Committing raised exception: %r', exc)
            await self.crash(exc)
            return False
        except IllegalStateError as exc:
            self.log.exception(
                'Got exception: %r\nCurrent assignment: %r',
                exc, self.assignment())
            await self.crash(exc)
            return False
        return True

    def verify_event_path(self, now: float, tp: TP) -> None:
        # long function ahead, but not difficult to test
        # as it always returns as soon as some condition is met.

        parent = cast(Consumer, self.consumer)
        app = parent.app
        consumer = self._ensure_consumer()
        monitor = app.monitor
        acks_enabled_for = app.topics.acks_enabled_for
        aiotp = parent._new_topicpartition(tp.topic, tp.partition)
        secs_since_started = now - self.time_started

        request_at = consumer.records_last_request.get(aiotp)
        if request_at is None:
            if secs_since_started >= self.tp_fetch_request_timeout_secs:
                # NO FETCH REQUEST SENT AT ALL SINCE WORKER START
                self.log.error(
                    SLOW_PROCESSING_NO_FETCH_SINCE_START,
                    tp, humanize_seconds_ago(secs_since_started),
                )
            return None

        response_at = consumer.records_last_response.get(aiotp)
        if response_at is None:
            if secs_since_started >= self.tp_fetch_response_timeout_secs:
                # NO FETCH RESPONSE RECEIVED AT ALL SINCE WORKER START
                self.log.error(
                    SLOW_PROCESSING_NO_RESPONSE_SINCE_START,
                    tp, humanize_seconds_ago(secs_since_started),
                )
            return None

        secs_since_request = now - request_at
        if secs_since_request >= self.tp_fetch_request_timeout_secs:
            # NO REQUEST SENT BY AIOKAFKA IN THE LAST n SECONDS
            self.log.error(
                SLOW_PROCESSING_NO_RECENT_FETCH,
                tp,
                humanize_seconds_ago(secs_since_request),
            )
            return None

        secs_since_response = now - response_at
        if secs_since_response >= self.tp_fetch_response_timeout_secs:
            # NO RESPONSE RECEIVED FROM KAKFA IN THE LAST n SECONDS
            self.log.error(
                SLOW_PROCESSING_NO_RECENT_RESPONSE,
                tp,
                humanize_seconds_ago(secs_since_response),
            )
            return None

        if monitor is not None:  # need for .stream_inbound_time
            highwater = self.highwater(tp)
            committed_offset = parent._committed_offset.get(tp)
            has_acks = acks_enabled_for(tp.topic)
            if highwater is None:
                if secs_since_started >= self.tp_stream_timeout_secs:
                    # AIOKAFKA HAS NOT UPDATED HIGHWATER SINCE STARTING
                    self.log.error(
                        SLOW_PROCESSING_NO_HIGHWATER_SINCE_START,
                        tp, humanize_seconds_ago(secs_since_started),
                    )
                return None

            if has_acks and committed_offset is not None:
                if highwater > committed_offset:
                    inbound = monitor.stream_inbound_time.get(tp)
                    if inbound is None:
                        if secs_since_started >= self.tp_stream_timeout_secs:
                            # AIOKAFKA IS FETCHING BUT STREAM IS NOT
                            # PROCESSING EVENTS (no events at all since
                            # start).
                            self._log_slow_processing_stream(
                                SLOW_PROCESSING_STREAM_IDLE_SINCE_START,
                                tp, humanize_seconds_ago(secs_since_started),
                            )
                        return None

                    secs_since_stream = now - inbound
                    if secs_since_stream >= self.tp_stream_timeout_secs:
                        # AIOKAFKA IS FETCHING, AND STREAM WAS WORKING
                        # BEFORE BUT NOW HAS STOPPED PROCESSING
                        # (or processing of an event in the stream takes
                        #  longer than tp_stream_timeout_secs).
                        self._log_slow_processing_stream(
                            SLOW_PROCESSING_STREAM_IDLE,
                            tp, humanize_seconds_ago(secs_since_stream),
                        )
                        return None

                    last_commit = self.tp_last_committed_at.get(tp)
                    if last_commit is None:
                        if secs_since_started >= self.tp_commit_timeout_secs:
                            # AIOKAFKA IS FETCHING AND STREAM IS PROCESSING
                            # BUT WE HAVE NOT COMMITTED ANYTHING SINCE WORKER
                            # START.
                            self._log_slow_processing_commit(
                                SLOW_PROCESSING_NO_COMMIT_SINCE_START,
                                tp, humanize_seconds_ago(secs_since_started),
                            )
                            return None
                    else:
                        secs_since_commit = now - last_commit
                        if secs_since_commit >= self.tp_commit_timeout_secs:
                            # AIOKAFKA IS FETCHING AND STREAM IS PROCESSING
                            # BUT WE HAVE NOT COMITTED ANYTHING IN A WHILE
                            # (commit offset is not advancing).
                            self._log_slow_processing_commit(
                                SLOW_PROCESSING_NO_RECENT_COMMIT,
                                tp, humanize_seconds_ago(secs_since_commit),
                            )
                            return None

    def _log_slow_processing_stream(self, msg: str, *args: Any) -> None:
        app = self.consumer.app
        self._log_slow_processing(
            msg, *args,
            causes=[
                SLOW_PROCESSING_CAUSE_STREAM,
                SLOW_PROCESSING_CAUSE_AGENT,
            ],
            setting='stream_processing_timeout',
            current_value=app.conf.stream_processing_timeout,
        )

    def _log_slow_processing_commit(self, msg: str, *args: Any) -> None:
        app = self.consumer.app
        self._log_slow_processing(
            msg, *args,
            causes=[SLOW_PROCESSING_CAUSE_COMMIT],
            setting='broker_commit_livelock_soft_timeout',
            current_value=app.conf.broker_commit_livelock_soft_timeout,
        )

    def _make_slow_processing_error(self,
                                    msg: str,
                                    causes: Iterable[str]) -> str:
        return ' '.join([
            msg,
            SLOW_PROCESSING_EXPLAINED,
            text.enumeration(causes, start=2, sep='\n\n'),
        ])

    def _log_slow_processing(self, msg: str, *args: Any,
                             causes: Iterable[str],
                             setting: str,
                             current_value: float) -> None:
        return self.log.error(
            self._make_slow_processing_error(msg, causes),
            *args,
            setting=setting,
            current_value=current_value,
        )

    async def position(self, tp: TP) -> Optional[int]:
        """Return the current position for topic partition."""
        return await self.call_thread(
            self._ensure_consumer().position, tp)

    async def seek_to_beginning(self, *partitions: _TopicPartition) -> None:
        """Seek list of offsets to the first available offset."""
        await self.call_thread(
            self._ensure_consumer().seek_to_beginning, *partitions)

    async def seek_wait(self, partitions: Mapping[TP, int]) -> None:
        """Seek partitions to specific offset and wait for operation."""
        consumer = self._ensure_consumer()
        await self.call_thread(self._seek_wait, consumer, partitions)

    async def _seek_wait(self,
                         consumer: Consumer,
                         partitions: Mapping[TP, int]) -> None:
        for tp, offset in partitions.items():
            self.log.dev('SEEK %r -> %r', tp, offset)
            consumer.seek(tp, offset)
            if offset > 0:
                self.consumer._read_offset[tp] = offset
        await asyncio.gather(*[
            consumer.position(tp) for tp in partitions
        ])

    def seek(self, partition: TP, offset: int) -> None:
        """Seek partition to specific offset."""
        self._ensure_consumer().seek(partition, offset)

    def assignment(self) -> Set[TP]:
        """Return the current assignment."""
        return ensure_TPset(self._ensure_consumer().assignment())

    def highwater(self, tp: TP) -> int:
        """Return the last offset in a specific partition."""
        if self.consumer.in_transaction:
            return self._ensure_consumer().last_stable_offset(tp)
        else:
            return self._ensure_consumer().highwater(tp)

    def topic_partitions(self, topic: str) -> Optional[int]:
        """Return the number of partitions configured for topic by name."""
        if self._consumer is not None:
            return self._consumer._coordinator._metadata_snapshot.get(topic)
        return None

    async def earliest_offsets(self,
                               *partitions: TP) -> Mapping[TP, int]:
        """Return the earliest offsets for a list of partitions."""
        return await self.call_thread(
            self._ensure_consumer().beginning_offsets, partitions)

    async def highwaters(self, *partitions: TP) -> Mapping[TP, int]:
        """Return the last offsets for a list of partitions."""
        return await self.call_thread(self._highwaters, partitions)

    async def _highwaters(self, partitions: List[TP]) -> Mapping[TP, int]:
        consumer = self._ensure_consumer()
        if self.consumer.in_transaction:
            return {
                tp: consumer.last_stable_offset(tp)
                for tp in partitions
            }
        else:
            return cast(Mapping[TP, int],
                        await consumer.end_offsets(partitions))

    def _ensure_consumer(self) -> aiokafka.AIOKafkaConsumer:
        if self._consumer is None:
            raise ConsumerNotStarted('Consumer thread not yet started')
        return self._consumer

    async def getmany(self,
                      active_partitions: Optional[Set[TP]],
                      timeout: float) -> RecordMap:
        """Fetch batch of messages from server."""
        # Implementation for the Fetcher service.
        _consumer = self._ensure_consumer()
        # NOTE: Since we are enqueing the fetch request,
        # we need to check when dequeued that we are not in a rebalancing
        # state at that point to return early, or we
        # will create a deadlock (fetch request starts after flow stopped)
        return await self.call_thread(
            self._fetch_records,
            _consumer,
            active_partitions,
            timeout=timeout,
            max_records=_consumer._max_poll_records,
        )

    async def _fetch_records(self,
                             consumer: aiokafka.AIOKafkaConsumer,
                             active_partitions: Set[TP],
                             timeout: float = None,
                             max_records: int = None) -> RecordMap:
        if not self.consumer.flow_active:
            return {}
        fetcher = consumer._fetcher
        if consumer._closed or fetcher._closed:
            raise ConsumerStoppedError()
        with fetcher._subscriptions.fetch_context():
            return await fetcher.fetched_records(
                active_partitions,
                timeout=timeout,
                max_records=max_records,
            )

    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 30.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        """Create/declare topic on server."""
        transport = cast(Transport, self.consumer.transport)
        _consumer = self._ensure_consumer()
        _retention = (int(want_seconds(retention) * 1000.0)
                      if retention else None)
        if len(topic) > TOPIC_LENGTH_MAX:
            raise ValueError(
                f'Topic name {topic!r} is too long (max={TOPIC_LENGTH_MAX})')
        await self.call_thread(
            transport._create_topic,
            self,
            _consumer._client,
            topic,
            partitions,
            replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=_retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    def key_partition(self,
                      topic: str,
                      key: Optional[bytes],
                      partition: int = None) -> Optional[int]:
        """Hash key to determine partition destination."""
        consumer = self._ensure_consumer()
        metadata = consumer._client.cluster
        partitions_for_topic = metadata.partitions_for_topic(topic)
        if partitions_for_topic is None:
            return None
        if partition is not None:
            assert partition >= 0
            assert partition in partitions_for_topic, \
                'Unrecognized partition'
            return partition

        all_partitions = list(partitions_for_topic)
        available = list(metadata.available_partitions_for_topic(topic))
        return self._partitioner(key, all_partitions, available)


class Producer(base.Producer):
    """Kafka producer using :pypi:`aiokafka`."""

    logger = logger

    allow_headers: bool = True
    _producer: Optional[aiokafka.AIOKafkaProducer] = None

    def __post_init__(self) -> None:
        self._send_on_produce_message = self.app.on_produce_message.send
        if self.partitioner is None:
            self.partitioner = DefaultPartitioner()
        if self._api_version != 'auto':
            wanted_api_version = parse_kafka_version(self._api_version)
            if wanted_api_version < (0, 11):
                self.allow_headers = False

    def _settings_default(self) -> Mapping[str, Any]:
        transport = cast(Transport, self.transport)
        return {
            'bootstrap_servers': server_list(
                transport.url, transport.default_port),
            'client_id': self.client_id,
            'acks': self.acks,
            'linger_ms': self.linger_ms,
            'max_batch_size': self.max_batch_size,
            'max_request_size': self.max_request_size,
            'compression_type': self.compression_type,
            'on_irrecoverable_error': self._on_irrecoverable_error,
            'security_protocol': 'SSL' if self.ssl_context else 'PLAINTEXT',
            'partitioner': self.partitioner,
            'request_timeout_ms': int(self.request_timeout * 1000),
            'api_version': self._api_version,
        }

    def _settings_auth(self) -> Mapping[str, Any]:
        return credentials_to_aiokafka_auth(
            self.credentials, self.ssl_context)

    async def begin_transaction(self, transactional_id: str) -> None:
        """Begin transaction by id."""
        await self._ensure_producer().begin_transaction(transactional_id)

    async def commit_transaction(self, transactional_id: str) -> None:
        """Commit transaction by id."""
        await self._ensure_producer().commit_transaction(transactional_id)

    async def abort_transaction(self, transactional_id: str) -> None:
        """Abort and rollback transaction by id."""
        await self._ensure_producer().abort_transaction(transactional_id)

    async def stop_transaction(self, transactional_id: str) -> None:
        """Stop transaction by id."""
        await self._ensure_producer().stop_transaction(transactional_id)

    async def maybe_begin_transaction(self, transactional_id: str) -> None:
        """Begin transaction (if one does not already exist)."""
        await self._ensure_producer().maybe_begin_transaction(transactional_id)

    async def commit_transactions(
            self,
            tid_to_offset_map: Mapping[str, Mapping[TP, int]],
            group_id: str,
            start_new_transaction: bool = True) -> None:
        """Commit transactions."""
        await self._ensure_producer().commit(
            tid_to_offset_map, group_id,
            start_new_transaction=start_new_transaction,
        )

    def _settings_extra(self) -> Mapping[str, Any]:
        if self.app.in_transaction:
            return {'acks': 'all'}
        return {}

    def _new_producer(self) -> aiokafka.AIOKafkaProducer:
        return self._producer_type(
            loop=self.loop,
            **{**self._settings_default(),
               **self._settings_auth(),
               **self._settings_extra()},
        )

    @property
    def _producer_type(self) -> Type[aiokafka.BaseProducer]:
        if self.app.in_transaction:
            return aiokafka.MultiTXNProducer
        return aiokafka.AIOKafkaProducer

    async def _on_irrecoverable_error(self, exc: BaseException) -> None:
        consumer = self.transport.app.consumer
        if consumer is not None:  # pragma: no cover
            # coverage executes this line, but does not mark as covered.
            await consumer.crash(exc)
        await self.crash(exc)

    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 20.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        """Create/declare topic on server."""
        _retention = (int(want_seconds(retention) * 1000.0)
                      if retention else None)
        producer = self._ensure_producer()
        await cast(Transport, self.transport)._create_topic(
            self,
            producer.client,
            topic,
            partitions,
            replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=_retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )
        await producer.client.force_metadata_update()  # Fixes #499

    def _ensure_producer(self) -> aiokafka.BaseProducer:
        if self._producer is None:
            raise NotReady('Producer service not yet started')
        return self._producer

    async def on_start(self) -> None:
        """Call when producer starts."""
        await super().on_start()
        producer = self._producer = self._new_producer()
        self.beacon.add(producer)
        await producer.start()

    async def on_stop(self) -> None:
        """Call when producer stops."""
        await super().on_stop()
        cast(Transport, self.transport)._topic_waiters.clear()
        producer, self._producer = self._producer, None
        if producer is not None:
            await producer.stop()

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int],
                   timestamp: Optional[float],
                   headers: Optional[HeadersArg],
                   *,
                   transactional_id: str = None) -> Awaitable[RecordMetadata]:
        """Schedule message to be transmitted by producer."""
        producer = self._ensure_producer()
        if headers is not None:
            if isinstance(headers, Mapping):
                headers = list(headers.items())
        self._send_on_produce_message(
            key=key, value=value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
        )
        if headers is not None and not self.allow_headers:
            headers = None
        timestamp_ms = int(timestamp * 1000.0) if timestamp else timestamp
        try:
            return cast(Awaitable[RecordMetadata], await producer.send(
                topic, value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=headers,
                transactional_id=transactional_id,
            ))
        except KafkaError as exc:
            raise ProducerSendError(f'Error while sending: {exc!r}') from exc

    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int],
                            timestamp: Optional[float],
                            headers: Optional[HeadersArg],
                            *,
                            transactional_id: str = None) -> RecordMetadata:
        """Send message and wait for it to be transmitted."""
        fut = await self.send(
            topic,
            key=key,
            value=value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
            transactional_id=transactional_id,
        )
        return await fut

    async def flush(self) -> None:
        """Wait for producer to finish transmitting all buffered messages."""
        await self.buffer.flush()
        if self._producer is not None:
            await self._producer.flush()

    def key_partition(self, topic: str, key: bytes) -> TP:
        """Hash key to determine partition destination."""
        producer = self._ensure_producer()
        partition = producer._partition(
            topic,
            partition=None,
            key=None,
            value=None,
            serialized_key=key,
            serialized_value=None,
        )
        return TP(topic, partition)

    def supports_headers(self) -> bool:
        """Return :const:`True` if message headers are supported."""
        producer = self._ensure_producer()
        client = producer.client
        if client is None:
            raise NotReady('Producer client not yet connected')
        return client.api_version >= (0, 11)


class Transport(base.Transport):
    """Kafka transport using :pypi:`aiokafka`."""

    Consumer: ClassVar[Type[ConsumerT]]
    Consumer = Consumer
    Producer: ClassVar[Type[ProducerT]]
    Producer = Producer

    default_port = 9092
    driver_version = f'aiokafka={aiokafka.__version__}'

    _topic_waiters: MutableMapping[str, StampedeWrapper]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._topic_waiters = {}

    def _topic_config(self,
                      retention: int = None,
                      compacting: bool = None,
                      deleting: bool = None) -> MutableMapping[str, Any]:
        config: MutableMapping[str, Any] = {}
        cleanup_flags: Set[str] = set()
        if compacting:
            cleanup_flags |= {'compact'}
        if deleting:
            cleanup_flags |= {'delete'}
        if cleanup_flags:
            config['cleanup.policy'] = ','.join(sorted(cleanup_flags))
        if retention:
            config['retention.ms'] = retention
        return config

    async def _create_topic(self,
                            owner: Service,
                            client: aiokafka.AIOKafkaClient,
                            topic: str,
                            partitions: int,
                            replication: int,
                            **kwargs: Any) -> None:
        assert topic is not None
        try:
            wrap = self._topic_waiters[topic]
        except KeyError:
            wrap = self._topic_waiters[topic] = StampedeWrapper(
                self._really_create_topic,
                owner,
                client,
                topic,
                partitions,
                replication,
                loop=asyncio.get_event_loop(), **kwargs)
        try:
            await wrap()
        except Exception:
            self._topic_waiters.pop(topic, None)
            raise

    async def _get_controller_node(
            self,
            owner: Service,
            client: aiokafka.AIOKafkaClient,
            timeout: int = 30000) -> Optional[int]:  # pragma: no cover
        nodes = [broker.nodeId for broker in client.cluster.brokers()]
        for node_id in nodes:
            if node_id is None:
                raise NotReady('Not connected to Kafka Broker')
            request = MetadataRequest_v1([])
            wait_result = await owner.wait(
                client.send(node_id, request),
                timeout=timeout,
            )
            if wait_result.stopped:
                owner.log.info(f'Shutting down - skipping creation.')
                return None
            response = wait_result.result
            return response.controller_id
        raise Exception(f'Controller node not found')

    async def _really_create_topic(
            self,
            owner: Service,
            client: aiokafka.AIOKafkaClient,
            topic: str,
            partitions: int,
            replication: int,
            *,
            config: Mapping[str, Any] = None,
            timeout: int = 30000,
            retention: int = None,
            compacting: bool = None,
            deleting: bool = None,
            ensure_created: bool = False) -> None:  # pragma: no cover
        owner.log.info('Creating topic %r', topic)

        if topic in client.cluster.topics():
            owner.log.debug('Topic %r exists, skipping creation.', topic)
            return

        protocol_version = 1
        extra_configs = config or {}
        config = self._topic_config(retention, compacting, deleting)
        config.update(extra_configs)

        controller_node = await self._get_controller_node(owner, client,
                                                          timeout=timeout)
        owner.log.debug('Found controller: %r', controller_node)

        if controller_node is None:
            if owner.should_stop:
                owner.log.info('Shutting down hence controller not found')
                return
            else:
                raise Exception('Controller node is None')

        request = CreateTopicsRequest[protocol_version](
            [(topic, partitions, replication, [], list(config.items()))],
            timeout,
            False,
        )
        wait_result = await owner.wait(
            client.send(controller_node, request),
            timeout=timeout,
        )
        if wait_result.stopped:
            owner.log.debug(f'Shutting down - skipping creation.')
            return
        response = wait_result.result

        assert len(response.topic_error_codes), 'single topic'

        _, code, reason = response.topic_error_codes[0]

        if code != 0:
            if not ensure_created and code == TopicExistsError.errno:
                owner.log.debug(
                    'Topic %r exists, skipping creation.', topic)
                return
            elif code == NotControllerError.errno:
                raise RuntimeError(f'Invalid controller: {controller_node}')
            else:
                raise for_code(code)(
                    f'Cannot create topic: {topic} ({code}): {reason}')
        else:
            owner.log.info('Topic %r created.', topic)
            return


def credentials_to_aiokafka_auth(credentials: CredentialsT = None,
                                 ssl_context: Any = None) -> Mapping:
    if credentials is not None:
        if isinstance(credentials, SSLCredentials):
            return {
                'security_protocol': credentials.protocol.value,
                'ssl_context': credentials.context,
            }
        elif isinstance(credentials, SASLCredentials):
            return {
                'security_protocol': credentials.protocol.value,
                'sasl_mechanism': credentials.mechanism.value,
                'sasl_plain_username': credentials.username,
                'sasl_plain_password': credentials.password,
                'ssl_context': credentials.ssl_context,
            }
        elif isinstance(credentials, GSSAPICredentials):
            return {
                'security_protocol': credentials.protocol.value,
                'sasl_mechanism': credentials.mechanism.value,
                'sasl_kerberos_service_name':
                    credentials.kerberos_service_name,
                'sasl_kerberos_domain_name':
                    credentials.kerberos_domain_name,
                'ssl_context': credentials.ssl_context,
            }
        else:
            raise ImproperlyConfigured(
                f'aiokafka does not support {credentials}')
    elif ssl_context is not None:
        return {
            'security_protocol': 'SSL',
            'ssl_context': ssl_context,
        }
    else:
        return {'security_protocol': 'PLAINTEXT'}
