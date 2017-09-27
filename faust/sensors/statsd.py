import typing
from time import monotonic
from typing import Any, cast
from mode import label
from .monitor import Monitor
from ..exceptions import ImproperlyConfigured
from ..types import CollectionT, EventT, Message, StreamT, TopicPartition
from ..types.transports import ConsumerT, ProducerT
from ..utils.objects import cached_property

try:
    import statsd
except ImportError:
    statsd = None

if typing.TYPE_CHECKING:
    from statsd import StatsClient
else:
    class StatsClient: ...  # noqa

__all__ = ['StatsdMonitor']


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
                 rate: float = 1.,
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
        return StatsClient(host=self.host, port=self.port, prefix=self.prefix)

    async def on_message_in(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message) -> None:
        await super().on_message_in(consumer_id, tp, offset, message)

        self.client.incr('messages_received', rate=self.rate)
        self.client.incr('messages_active', rate=self.rate)
        self.client.incr(f'topic.{tp.topic}.messages_received', rate=self.rate)

    async def on_stream_event_in(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        await super().on_stream_event_in(tp, offset, stream, event)
        self.client.incr('events', rate=self.rate)
        self.client.incr(
            f'stream.{self._sanitize(label(stream))}.events', rate=self.rate)
        self.client.incr(
            f'task.{self._sanitize(label(stream.task_owner))}.events',
            rate=self.rate,
        )
        self.client.incr('events_active', rate=self.rate)

    async def on_stream_event_out(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        await super().on_stream_event_out(tp, offset, stream, event)
        self.client.decr('events_active', rate=self.rate)
        self.client.timing('events_runtime', self._time(
            self.events_runtime[-1]), rate=self.rate)

    async def on_message_out(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message = None) -> None:
        await super().on_message_out(consumer_id, tp, offset, message)
        self.client.decr('messages_active', rate=self.rate)

    def on_table_get(self, table: CollectionT, key: Any) -> None:
        super().on_table_get(table, key)
        self.client.incr('table.{}.keys_retrieved'.format(table.name),
                         rate=self.rate)

    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        super().on_table_set(table, key, value)
        self.client.incr('table.{}.keys_updated'.format(table.name),
                         rate=self.rate)

    def on_table_del(self, table: CollectionT, key: Any) -> None:
        super().on_table_del(table, key)
        self.client.incr('table.{}.keys_deleted'.format(table.name),
                         rate=self.rate)

    async def on_commit_completed(
            self, consumer: ConsumerT, state: Any) -> None:
        await super().on_commit_completed(consumer, state)
        self.client.timing('commit_latency', self._time(
            monotonic() - cast(float, state)), rate=self.rate)

    async def on_send_initiated(self, producer: ProducerT, topic: str,
                                keysize: int, valsize: int) -> Any:

        self.client.incr(f'topic.{topic}.messages_sent', rate=self.rate)
        return await super().on_send_initiated(producer,
                                               topic, keysize, valsize)

    async def on_send_completed(
            self, producer: ProducerT, state: Any) -> None:
        await super().on_send_completed(producer, state)
        self.client.incr('messages_sent', rate=self.rate)
        self.client.timing('send_latency', self._time(
            monotonic() - cast(float, state)), rate=self.rate)

    def _sanitize(self, name: str) -> str:
        name = name.replace('<', '')
        name = name.replace('>', '')
        name = name.replace(' ', '')
        return name.replace(':', '-')

    def _time(self, time: float) -> float:
        return time * 1000.

    @cached_property
    def client(self) -> StatsClient:
        return self._new_statsd_client()
