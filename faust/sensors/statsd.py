"""Monitor using Statsd."""
import re
import typing

from typing import Any, Dict, Optional, Pattern, cast

from mode.utils.objects import cached_property

from faust import web
from faust.exceptions import ImproperlyConfigured
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
from faust.types.assignor import PartitionAssignorT
from faust.types.transports import ConsumerT, ProducerT

from .monitor import Monitor, TPOffsetMapping

try:
    import statsd
except ImportError:  # pragma: no cover
    statsd = None

if typing.TYPE_CHECKING:  # pragma: no cover
    from statsd import StatsClient
else:
    class StatsClient: ...  # noqa

__all__ = ['StatsdMonitor']

# This regular expression is used to generate stream ids in Statsd.
# It converts for example
#    "Stream: <Topic: withdrawals>"
# -> "Stream_Topic_withdrawals"
#
# See StatsdMonitor._normalize()
RE_NORMALIZE = re.compile(r'[\<\>:\s]+')
RE_NORMALIZE_SUBSTITUTION = '_'


class StatsdMonitor(Monitor):
    """Statsd Faust Sensor.

    This sensor, records statistics to Statsd along with computing metrics
    for the stats server
    """

    host: str
    port: int
    prefix: str

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 8125,
                 prefix: str = 'faust-app',
                 rate: float = 1.0,
                 **kwargs: Any) -> None:
        self.host = host
        self.port = port
        self.prefix = prefix
        self.rate = rate
        if statsd is None:
            raise ImproperlyConfigured(
                'StatsMonitor requires `pip install statsd`.')
        super().__init__(**kwargs)

    def _new_statsd_client(self) -> StatsClient:
        return statsd.StatsClient(
            host=self.host, port=self.port, prefix=self.prefix)

    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        """Call before message is delegated to streams."""
        super().on_message_in(tp, offset, message)

        self.client.incr('messages_received', rate=self.rate)
        self.client.incr('messages_active', rate=self.rate)
        self.client.incr(f'topic.{tp.topic}.messages_received', rate=self.rate)
        self.client.gauge(f'read_offset.{tp.topic}.{tp.partition}', offset)

    def on_stream_event_in(self, tp: TP, offset: int, stream: StreamT,
                           event: EventT) -> Optional[Dict]:
        """Call when stream starts processing an event."""
        state = super().on_stream_event_in(tp, offset, stream, event)
        self.client.incr('events', rate=self.rate)
        self.client.incr(
            f'stream.{self._stream_label(stream)}.events',
            rate=self.rate,
        )
        self.client.incr('events_active', rate=self.rate)
        return state

    def _stream_label(self, stream: StreamT) -> str:
        return self._normalize(
            stream.shortlabel.lstrip('Stream:'),
        ).strip('_').lower()

    def on_stream_event_out(self, tp: TP, offset: int, stream: StreamT,
                            event: EventT, state: Dict = None) -> None:
        """Call when stream is done processing an event."""
        super().on_stream_event_out(tp, offset, stream, event, state)
        self.client.decr('events_active', rate=self.rate)
        self.client.timing(
            'events_runtime',
            self.secs_to_ms(self.events_runtime[-1]),
            rate=self.rate)

    def on_message_out(self,
                       tp: TP,
                       offset: int,
                       message: Message) -> None:
        """Call when message is fully acknowledged and can be committed."""
        super().on_message_out(tp, offset, message)
        self.client.decr('messages_active', rate=self.rate)

    def on_table_get(self, table: CollectionT, key: Any) -> None:
        """Call when value in table is retrieved."""
        super().on_table_get(table, key)
        self.client.incr(f'table.{table.name}.keys_retrieved', rate=self.rate)

    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        """Call when new value for key in table is set."""
        super().on_table_set(table, key, value)
        self.client.incr(f'table.{table.name}.keys_updated', rate=self.rate)

    def on_table_del(self, table: CollectionT, key: Any) -> None:
        """Call when key in a table is deleted."""
        super().on_table_del(table, key)
        self.client.incr(f'table.{table.name}.keys_deleted', rate=self.rate)

    def on_commit_completed(self, consumer: ConsumerT, state: Any) -> None:
        """Call when consumer commit offset operation completed."""
        super().on_commit_completed(consumer, state)
        self.client.timing(
            'commit_latency',
            self.ms_since(cast(float, state)),
            rate=self.rate)

    def on_send_initiated(self, producer: ProducerT, topic: str,
                          message: PendingMessage,
                          keysize: int, valsize: int) -> Any:
        """Call when message added to producer buffer."""
        self.client.incr(f'topic.{topic}.messages_sent', rate=self.rate)
        return super().on_send_initiated(
            producer, topic, message, keysize, valsize)

    def on_send_completed(self,
                          producer: ProducerT,
                          state: Any,
                          metadata: RecordMetadata) -> None:
        """Call when producer finished sending message."""
        super().on_send_completed(producer, state, metadata)
        self.client.incr('messages_sent', rate=self.rate)
        self.client.timing(
            'send_latency',
            self.ms_since(cast(float, state)),
            rate=self.rate)

    def on_send_error(self,
                      producer: ProducerT,
                      exc: BaseException,
                      state: Any) -> None:
        """Call when producer was unable to publish message."""
        super().on_send_error(producer, exc, state)
        self.client.incr('messages_sent_error', rate=self.rate)
        self.client.timing(
            'send_latency_for_error',
            self.ms_since(cast(float, state)),
            rate=self.rate)

    def on_assignment_error(self,
                            assignor: PartitionAssignorT,
                            state: Dict,
                            exc: BaseException) -> None:
        """Partition assignor did not complete assignor due to error."""
        super().on_assignment_error(assignor, state, exc)
        self.client.incr('assignments_error', rate=self.rate)
        self.client.timing(
            'assignment_latency',
            self.ms_since(state['time_start']),
            rate=self.rate)

    def on_assignment_completed(self,
                                assignor: PartitionAssignorT,
                                state: Dict) -> None:
        """Partition assignor completed assignment."""
        super().on_assignment_completed(assignor, state)
        self.client.incr('assignments_complete', rate=self.rate)
        self.client.timing(
            'assignment_latency',
            self.ms_since(state['time_start']),
            rate=self.rate)

    def on_rebalance_start(self, app: AppT) -> Dict:
        """Cluster rebalance in progress."""
        state = super().on_rebalance_start(app)
        self.client.incr('rebalances', rate=self.rate)
        return state

    def on_rebalance_return(self, app: AppT, state: Dict) -> None:
        """Consumer replied assignment is done to broker."""
        super().on_rebalance_return(app, state)
        self.client.decr('rebalances', rate=self.rate)
        self.client.incr('rebalances_recovering', rate=self.rate)
        self.client.timing(
            'rebalance_return_latency',
            self.ms_since(state['time_return']),
            rate=self.rate)

    def on_rebalance_end(self, app: AppT, state: Dict) -> None:
        """Cluster rebalance fully completed (including recovery)."""
        super().on_rebalance_end(app, state)
        self.client.decr('rebalances_recovering', rate=self.rate)
        self.client.timing(
            'rebalance_end_latency',
            self.ms_since(state['time_end']),
            rate=self.rate)

    def count(self, metric_name: str, count: int = 1) -> None:
        """Count metric by name."""
        super().count(metric_name, count=count)
        self.client.incr(metric_name, count=count, rate=self.rate)

    def on_tp_commit(self, tp_offsets: TPOffsetMapping) -> None:
        """Call when offset in topic partition is committed."""
        super().on_tp_commit(tp_offsets)
        for tp, offset in tp_offsets.items():
            metric_name = f'committed_offset.{tp.topic}.{tp.partition}'
            self.client.gauge(metric_name, offset)

    def track_tp_end_offset(self, tp: TP, offset: int) -> None:
        """Track new topic partition end offset for monitoring lags."""
        super().track_tp_end_offset(tp, offset)
        metric_name = f'end_offset.{tp.topic}.{tp.partition}'
        self.client.gauge(metric_name, offset)

    def on_web_request_end(self,
                           app: AppT,
                           request: web.Request,
                           response: Optional[web.Response],
                           state: Dict,
                           *,
                           view: web.View = None) -> None:
        """Web server finished working on request."""
        super().on_web_request_end(app, request, response, state, view=view)
        status_code = int(state['status_code'])
        self.client.incr(f'http_status_code.{status_code}', rate=self.rate)
        self.client.timing(
            'http_response_latency',
            self.ms_since(state['time_end']),
            rate=self.rate)

    def _normalize(self, name: str,
                   *,
                   pattern: Pattern = RE_NORMALIZE,
                   substitution: str = RE_NORMALIZE_SUBSTITUTION) -> str:
        return pattern.sub(substitution, name)

    @cached_property
    def client(self) -> StatsClient:
        """Return statsd client."""
        return self._new_statsd_client()
