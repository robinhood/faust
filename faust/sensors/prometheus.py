"""Monitor using Promethus."""
import typing

from typing import Any, cast

from faust.exceptions import ImproperlyConfigured
from faust import web

from aiohttp.web import Response

from faust.types.assignor import PartitionAssignorT
from faust.types.transports import ConsumerT, ProducerT
from faust import web as _web
from faust.types import (
    AppT,
    CollectionT,
    EventT,
    Message,
    PendingMessage,
    RecordMetadata,
    StreamT,
    TP,
)

from .monitor import Monitor, TPOffsetMapping

try:
    import prometheus_client
    from prometheus_client import (
        Counter, Gauge, Histogram, generate_latest, REGISTRY)
except ImportError:  # pragma: no cover
    prometheus_client = None


__all__ = ['PrometheusMonitor']


class PrometheusMonitor(Monitor):
    """
    Prometheus Faust Sensor.

    This sensor, records statistics using prometheus_client and expose
    them using the aiohttp server running under /metrics by default

    Usage:
        import faust
        from faust.sensors.prometheus import PrometheusMonitor

        app = faust.App('example', broker='kafka://')
        app.monitor = PrometheusMonitor(app, pattern='/metrics')
    """

    ERROR = 'error'
    COMPLETED = 'completed'
    KEYS_RETRIEVED = 'keys_retrieved'
    KEYS_UPDATED = 'keys_updated'
    KEYS_DELETED = 'keys_deleted'

    def __init__(self, app: AppT,
                 pattern: str = '/metrics', **kwargs: Any) -> None:
        self.app = app
        self.pattern = pattern

        if prometheus_client is None:
            raise ImproperlyConfigured(
                'prometheus_client requires `pip install prometheus_client`.')

        self._initialize_metrics()
        self.expose_metrics()
        super().__init__(**kwargs)

    def _initialize_metrics(self) -> None:
        """
        Initialize Prometheus metrics
        """
        # On message received
        self.messages_received = Counter(
            'messages_received', 'Total messages received')
        self.active_messages = Gauge(
            'active_messages', 'Total active messages')
        self.messages_received_per_topics = Counter(
            'messages_received_per_topic',
            'Messages received per topic', ['topic'])
        self.messages_received_per_topics_partition = Gauge(
            'messages_received_per_topics_partition',
            'Messages received per topic/partition', ['topic', 'partition'])
        self.events_runtime_latency = Histogram(
            'events_runtime_ms', 'Events runtime in ms')

        # On Event Stream in
        self.total_events = Counter(
            'total_events', 'Total events received')
        self.total_active_events = Gauge(
            'total_active_events', 'Total active events')
        self.total_events_per_stream = Counter(
            'total_events_per_stream',
            'Events received per Stream', ['stream'])

        # On table changes get/set/del keys
        self.table_operations = Counter(
            'table_operations', 'Total table operations',
            ['table', 'operation'])

        # On message send
        self.topic_messages_sent = Counter(
            'topic_messages_sent', 'Total messages sent per topic', ['topic'])
        self.total_sent_messages = Counter(
            'total_sent_messages', 'Total messages sent')
        self.producer_send_latency = Histogram(
            'producer_send_latency', 'Producer send latency in ms')
        self.total_error_messages_sent = Counter(
            'total_error_messages_sent', 'Total error messages sent')
        self.producer_error_send_latency = Histogram(
            'producer_error_send_latency', 'Producer error send latency in ms')

        # Assignment
        self.assignment_operations = Counter(
            'assignment_operations',
            'Total assigment operations (completed/error)',
            ['operation'])
        self.assign_latency = Histogram(
            'assign_latency', 'Assignment latency in ms')

        # Revalances
        self.total_rebalances = Gauge(
            'total_rebalances', 'Total rebalances')
        self.total_rebalances_recovering = Gauge(
            'total_rebalances_recovering', 'Total rebalances recovering')
        self.revalance_done_consumer_latency = Histogram(
            'revalance_done_consumer_latency',
            'Consumer replying that rebalance is done to broker in ms')
        self.revalance_done_latency = Histogram(
            'revalance_done_latency',
            'Revalance finished latency in ms')

        # Count Metrics by name
        self.count_metrics_by_name = Gauge(
            'metrics_by_name',
            'Total metrics by name',
            ['metric'])

        # Web
        self.http_status_codes = Counter(
            'http_status_codes',
            'Total http_status code',
            ['status_code'])
        self.http_latency = Histogram(
            'http_latency',
            'Http response latency in ms')

        # Topic/Partition Offsets
        self.topic_partition_end_offset = Gauge(
            'topic_partition_end_offset',
            'Offset ends per topic/partition',
            ['topic', 'partition'])
        self.topic_partition_offset_commited = Gauge(
            'topic_partition_offset_commited',
            'Offset commited per topic/partition',
            ['topic', 'partition'])
        self.consumer_commit_latency = Histogram(
            'consumer_commit_latency', 'Consumer commit latency in ms')

    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        """Call before message is delegated to streams."""
        super().on_message_in(tp, offset, message)

        self.messages_received.inc()
        self.active_messages.inc()
        self.messages_received_per_topics.labels(topic=tp.topic).inc()
        self.messages_received_per_topics_partition.labels(
            topic=tp.topic, partition=tp.partition).set(offset)

    def on_stream_event_in(self, tp: TP, offset: int, stream: StreamT,
                           event: EventT) -> typing.Optional[typing.Dict]:
        """Call when stream starts processing an event."""
        state = super().on_stream_event_in(tp, offset, stream, event)
        self.total_events.inc()
        self.total_active_events.inc()
        self.total_events_per_stream.labels(
            stream=f'stream.{self._stream_label(stream)}.events').inc()

        return state

    def _stream_label(self, stream: StreamT) -> str:
        return self._normalize(
            stream.shortlabel.lstrip('Stream:'),
        ).strip('_').lower()

    def on_stream_event_out(self, tp: TP, offset: int, stream: StreamT,
                            event: EventT, state: typing.Dict = None) -> None:
        """Call when stream is done processing an event."""
        super().on_stream_event_out(tp, offset, stream, event, state)
        self.total_active_events.dec()
        self.events_runtime_latency.observe(
            self.secs_to_ms(self.events_runtime[-1]))

    def on_message_out(self,
                       tp: TP,
                       offset: int,
                       message: Message) -> None:
        """Call when message is fully acknowledged and can be committed."""
        super().on_message_out(tp, offset, message)
        self.active_messages.dec()

    def on_table_get(self, table: CollectionT, key: typing.Any) -> None:
        """Call when value in table is retrieved."""
        super().on_table_get(table, key)
        self.table_operations.labels(
            table=f'table.{table.name}',
            operation=self.KEYS_RETRIEVED).inc()

    def on_table_set(self, table: CollectionT, key: typing.Any,
                     value: typing.Any) -> None:
        """Call when new value for key in table is set."""
        super().on_table_set(table, key, value)
        self.table_operations.labels(
            table=f'table.{table.name}',
            operation=self.KEYS_UPDATED).inc()

    def on_table_del(self, table: CollectionT, key: typing.Any) -> None:
        """Call when key in a table is deleted."""
        super().on_table_del(table, key)
        self.table_operations.labels(
            table=f'table.{table.name}',
            operation=self.KEYS_DELETED).inc()

    def on_commit_completed(self, consumer: ConsumerT,
                            state: typing.Any) -> None:
        """Call when consumer commit offset operation completed."""
        super().on_commit_completed(consumer, state)
        self.consumer_commit_latency.observe(
            self.ms_since(typing.cast(float, state)))

    def on_send_initiated(self, producer: ProducerT, topic: str,
                          message: PendingMessage,
                          keysize: int, valsize: int) -> typing.Any:
        """Call when message added to producer buffer."""
        self.topic_messages_sent.labels(topic=f'topic.{topic}').inc()

        return super().on_send_initiated(
            producer, topic, message, keysize, valsize)

    def on_send_completed(self,
                          producer: ProducerT,
                          state: typing.Any,
                          metadata: RecordMetadata) -> None:
        """Call when producer finished sending message."""
        super().on_send_completed(producer, state, metadata)
        self.total_sent_messages.inc()
        self.producer_send_latency.observe(
            self.ms_since(typing.cast(float, state)))

    def on_send_error(self,
                      producer: ProducerT,
                      exc: BaseException,
                      state: typing.Any) -> None:
        """Call when producer was unable to publish message."""
        super().on_send_error(producer, exc, state)
        self.total_error_messages_sent.inc()
        self.producer_error_send_latency.observe(
            self.ms_since(typing.cast(float, state)))

    def on_assignment_error(self,
                            assignor: PartitionAssignorT,
                            state: typing.Dict,
                            exc: BaseException) -> None:
        """Partition assignor did not complete assignor due to error."""
        super().on_assignment_error(assignor, state, exc)
        self.assignment_operations.labels(operation=self.ERROR).inc()
        self.assign_latency.observe(
            self.ms_since(state['time_start']))

    def on_assignment_completed(self,
                                assignor: PartitionAssignorT,
                                state: typing. Dict) -> None:
        """Partition assignor completed assignment."""
        super().on_assignment_completed(assignor, state)
        self.assignment_operations.labels(operation=self.COMPLETED).inc()
        self.assign_latency.observe(
            self.ms_since(state['time_start']))

    def on_rebalance_start(self, app: AppT) -> typing.Dict:
        """Cluster rebalance in progress."""
        state = super().on_rebalance_start(app)
        self.total_rebalances.inc()

        return state

    def on_rebalance_return(self, app: AppT, state: typing.Dict) -> None:
        """Consumer replied assignment is done to broker."""
        super().on_rebalance_return(app, state)
        self.total_rebalances.dec()
        self.total_rebalances_recovering.inc()
        self.revalance_done_consumer_latency.observe(
            self.ms_since(state['time_return']))

    def on_rebalance_end(self, app: AppT, state: typing.Dict) -> None:
        """Cluster rebalance fully completed (including recovery)."""
        super().on_rebalance_end(app, state)
        self.total_rebalances_recovering.dec()
        self.revalance_done_latency.observe(
            self.ms_since(state['time_end']))

    def count(self, metric_name: str, count: int = 1) -> None:
        """Count metric by name."""
        super().count(metric_name, count=count)
        self.count_metrics_by_name.labels(metric=metric_name).inc(count)

    def on_tp_commit(self, tp_offsets: TPOffsetMapping) -> None:
        """Call when offset in topic partition is committed."""
        super().on_tp_commit(tp_offsets)
        for tp, offset in tp_offsets.items():
            self.topic_partition_offset_commited.labels(
                topic=tp.topic, partition=tp.partition).set(offset)

    def track_tp_end_offset(self, tp: TP, offset: int) -> None:
        """Track new topic partition end offset for monitoring lags."""
        super().track_tp_end_offset(tp, offset)
        self.topic_partition_end_offset.labels(
            topic=tp.topic, partition=tp.partition).set(offset)

    def on_web_request_end(self,
                           app: AppT,
                           request: web.Request,
                           response: typing.Optional[web.Response],
                           state: typing.Dict,
                           *,
                           view: web.View = None) -> None:
        """Web server finished working on request."""
        super().on_web_request_end(app, request, response, state, view=view)
        status_code = int(state['status_code'])
        self.http_status_codes.labels(status_code=status_code).inc()
        self.http_latency.observe(
            self.ms_since(state['time_end']))

    def expose_metrics(self) -> None:
        """Expose promethues metrics using the current aiohttp application."""
        @self.app.page(self.pattern)
        async def metrics_handler(self: _web.View,
                                  request: _web.Request) -> _web.Response:
            headers = {
                'Content-Type': 'text/plain; version=0.0.4; charset=utf-8',
            }

            return cast(_web.Response, Response(
                body=generate_latest(REGISTRY), headers=headers, status=200))
