"""Monitor using Statsd."""
import re
import typing
from time import monotonic
from typing import Any, Pattern, cast

from mode.utils.objects import cached_property

from faust.exceptions import ImproperlyConfigured
from faust.types import CollectionT, EventT, Message, StreamT, TP
from faust.types.transports import ConsumerT, ProducerT

from .monitor import Monitor, TPOffsetMapping

try:
    import statsd
except ImportError:
    statsd = None

if typing.TYPE_CHECKING:
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
        super().on_message_in(tp, offset, message)

        self.client.incr('messages_received', rate=self.rate)
        self.client.incr('messages_active', rate=self.rate)
        self.client.incr(f'topic.{tp.topic}.messages_received', rate=self.rate)
        self.client.gauge(f'read_offset.{tp.topic}.{tp.partition}', offset)

    def on_stream_event_in(self, tp: TP, offset: int, stream: StreamT,
                           event: EventT) -> None:
        super().on_stream_event_in(tp, offset, stream, event)
        self.client.incr('events', rate=self.rate)
        self.client.incr(
            f'stream.{self._stream_label(stream)}.events',
            rate=self.rate,
        )
        self.client.incr('events_active', rate=self.rate)

    def _stream_label(self, stream: StreamT) -> str:
        return self._normalize(
            stream.shortlabel.lstrip('Stream:'),
        ).strip('_').lower()

    def on_stream_event_out(self, tp: TP, offset: int, stream: StreamT,
                            event: EventT) -> None:
        super().on_stream_event_out(tp, offset, stream, event)
        self.client.decr('events_active', rate=self.rate)
        self.client.timing(
            'events_runtime',
            self._time(self.events_runtime[-1]),
            rate=self.rate)

    def on_message_out(self,
                       tp: TP,
                       offset: int,
                       message: Message) -> None:
        super().on_message_out(tp, offset, message)
        self.client.decr('messages_active', rate=self.rate)

    def on_table_get(self, table: CollectionT, key: Any) -> None:
        super().on_table_get(table, key)
        self.client.incr(f'table.{table.name}.keys_retrieved', rate=self.rate)

    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        super().on_table_set(table, key, value)
        self.client.incr(f'table.{table.name}.keys_updated', rate=self.rate)

    def on_table_del(self, table: CollectionT, key: Any) -> None:
        super().on_table_del(table, key)
        self.client.incr(f'table.{table.name}.keys_deleted', rate=self.rate)

    def on_commit_completed(self, consumer: ConsumerT, state: Any) -> None:
        super().on_commit_completed(consumer, state)
        self.client.timing(
            'commit_latency',
            self._time(monotonic() - cast(float, state)),
            rate=self.rate)

    def on_send_initiated(self, producer: ProducerT, topic: str,
                          keysize: int, valsize: int) -> Any:
        self.client.incr(f'topic.{topic}.messages_sent', rate=self.rate)
        return super().on_send_initiated(producer, topic, keysize, valsize)

    def on_send_completed(self, producer: ProducerT, state: Any) -> None:
        super().on_send_completed(producer, state)
        self.client.incr('messages_sent', rate=self.rate)
        self.client.timing(
            'send_latency',
            self._time(monotonic() - cast(float, state)),
            rate=self.rate)

    def count(self, metric_name: str, count: int = 1) -> None:
        super().count(metric_name, count=count)
        self.client.incr(metric_name, count=count, rate=self.rate)

    def on_tp_commit(self, tp_offsets: TPOffsetMapping) -> None:
        super().on_tp_commit(tp_offsets)
        for tp, offset in tp_offsets.items():
            metric_name = f'committed_offset.{tp.topic}.{tp.partition}'
            self.client.gauge(metric_name, offset)

    def track_tp_end_offset(self, tp: TP, offset: int) -> None:
        super().track_tp_end_offset(tp, offset)
        metric_name = f'end_offset.{tp.topic}.{tp.partition}'
        self.client.gauge(metric_name, offset)

    def _normalize(self, name: str,
                   *,
                   pattern: Pattern = RE_NORMALIZE,
                   substitution: str = RE_NORMALIZE_SUBSTITUTION) -> str:
        return pattern.sub(substitution, name)

    def _time(self, time: float) -> float:
        return time * 1000.

    @cached_property
    def client(self) -> StatsClient:
        return self._new_statsd_client()
