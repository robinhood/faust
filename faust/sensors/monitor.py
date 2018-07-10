"""Monitor - sensor tracking metrics."""
import asyncio
import statistics
from time import monotonic
from typing import Any, Callable, List, Mapping, MutableMapping, cast

from mode import Service, ServiceT, label
from mode.proxy import ServiceProxy
from mode.utils.compat import Counter
from mode.utils.objects import KeywordReduce, cached_property

from faust.types import CollectionT, EventT, Message, StreamT, TP, TopicT
from faust.types.transports import ConsumerT, ProducerT

from .base import Sensor

__all__ = [
    'TableState',
    'Monitor',
    'MonitorService',
]

MAX_AVG_HISTORY = 100
MAX_COMMIT_LATENCY_HISTORY = 30
MAX_SEND_LATENCY_HISTORY = 30


class TableState(KeywordReduce):
    """Represents the current state of a table."""

    #: The table this object records statistics for.
    # the attribute here cannot be None, but need to exist on the
    # class so that Sphinx finds it, so we cast the None to its type.
    table: CollectionT = cast(CollectionT, None)

    #: Number of times a key has been retrieved from this table.
    keys_retrieved: int = 0

    #: Number of times a key has been created/changed in this table.
    keys_updated: int = 0

    #: Number of times a key has been deleted from this table.
    keys_deleted: int = 0

    def __init__(self,
                 table: CollectionT,
                 *,
                 keys_retrieved: int = 0,
                 keys_updated: int = 0,
                 keys_deleted: int = 0) -> None:
        self.table: CollectionT = table
        self.keys_retrieved = keys_retrieved
        self.keys_updated = keys_updated
        self.keys_deleted = keys_deleted

    def asdict(self) -> Mapping:
        return {
            'keys_retrieved': self.keys_retrieved,
            'keys_updated': self.keys_updated,
            'keys_deleted': self.keys_deleted,
        }

    def __reduce_keywords__(self) -> Mapping:
        return {**self.asdict(), 'table': self.table}


class Monitor(ServiceProxy, Sensor, KeywordReduce):
    """Default Faust Sensor.

    This is the default sensor, recording statistics about
    events, etc.
    """

    #: Max number of total run time values to keep to build average.
    max_avg_history: int = 0

    #: Max number of commit latency numbers to keep.
    max_commit_latency_history: int = 0

    #: Max number of send latency numbers to keep.
    max_send_latency_history: int = 0

    #: Mapping of tables
    tables: MutableMapping[str, TableState] = cast(
        MutableMapping[str, TableState], None)

    #: Number of messages currently being processed.
    messages_active: int = 0

    #: Number of messages processed in total.
    messages_received_total: int = 0

    #: Count of messages received by topic
    messages_received_by_topic: Counter[str] = cast(Counter[str], None)

    #: Number of messages being processed this second.
    messages_s: int = 0

    #: Number of messages sent in total.
    messages_sent: int = 0

    #: Number of messages sent by topic.
    messages_sent_by_topic: Counter[str] = cast(Counter[str], None)

    #: Number of events currently being processed.
    events_active: int = 0

    #: Number of events processed in total.
    events_total: int = 0

    #: Number of events being processed this second.
    events_s: int = 0

    #: Count of events processed by stream
    events_by_stream: Counter[str] = cast(Counter[str], None)

    #: Count of events processed by task
    events_by_task: Counter[str] = cast(Counter[str], None)

    #: Average event runtime over the last second.
    events_runtime_avg: float = 0.0

    #: List of run times used for averages
    events_runtime: List[float] = cast(List[float], None)

    #: List of commit latency values
    commit_latency: List[float] = cast(List[float], None)

    #: List of send latency values
    send_latency: List[float] = cast(List[float], None)

    #: Counter of times a topics buffer was full
    topic_buffer_full: Counter[TopicT] = cast(Counter[TopicT], None)

    #: Arbitrary counts added by apps
    metric_counts: Counter[str] = cast(Counter[str], None)

    def __init__(self,
                 *,
                 max_avg_history: int = MAX_AVG_HISTORY,
                 max_commit_latency_history: int = MAX_COMMIT_LATENCY_HISTORY,
                 max_send_latency_history: int = MAX_SEND_LATENCY_HISTORY,
                 messages_sent: int = 0,
                 tables: MutableMapping[str, TableState] = None,
                 messages_active: int = 0,
                 events_active: int = 0,
                 messages_received_total: int = 0,
                 messages_received_by_topic: Counter[str] = None,
                 events_total: int = 0,
                 events_by_stream: Counter[StreamT] = None,
                 events_by_task: Counter[asyncio.Task] = None,
                 events_runtime: List[float] = None,
                 commit_latency: List[float] = None,
                 send_latency: List[float] = None,
                 events_s: int = 0,
                 messages_s: int = 0,
                 events_runtime_avg: float = 0.0,
                 topic_buffer_full: Counter[TopicT] = None,
                 **kwargs: Any) -> None:
        self.max_avg_history = max_avg_history
        self.max_commit_latency_history = max_commit_latency_history
        self.max_send_latency_history = max_send_latency_history

        self.tables = {} if tables is None else tables
        self.commit_latency = [] if commit_latency is None else commit_latency
        self.send_latency = [] if send_latency is None else send_latency

        self.messages_active = messages_active
        self.messages_received_total = messages_received_total
        self.messages_received_by_topic = Counter()
        self.messages_sent = messages_sent
        self.messages_sent_by_topic = Counter()
        self.messages_s = messages_s

        self.events_active = events_active
        self.events_total = events_total
        self.events_by_task = Counter()
        self.events_by_stream = Counter()
        self.events_s = events_s
        self.events_runtime_avg = events_runtime_avg
        self.events_runtime = [] if events_runtime is None else events_runtime
        self.topic_buffer_full = Counter()
        self.time: Callable[[], float] = monotonic

        self.metric_counts = Counter()

    def asdict(self) -> Mapping:
        return {
            'messages_active': self.messages_active,
            'messages_received_total': self.messages_received_total,
            'messages_sent': self.messages_sent,
            'messages_sent_by_topic': self.messages_sent_by_topic,
            'messages_s': self.messages_s,
            'messages_received_by_topic': self.messages_received_by_topic,
            'events_active': self.events_active,
            'events_total': self.events_total,
            'events_s': self.events_s,
            'events_runtime_avg': self.events_runtime_avg,
            'events_by_task': self._events_by_task_dict(),
            'events_by_stream': self._events_by_stream_dict(),
            'commit_latency': self.commit_latency,
            'send_latency': self.send_latency,
            'topic_buffer_full': self._topic_buffer_full_dict(),
            'tables': {
                name: table.asdict() for name, table in self.tables.items()
            },
            'metric_counts': self._metric_counts_dict(),
        }

    def _events_by_stream_dict(self) -> MutableMapping[str, int]:
        return {label(stream): count
                for stream, count in self.events_by_stream.items()}

    def _events_by_task_dict(self) -> MutableMapping[str, int]:
        return {label(task): count
                for task, count in self.events_by_task.items()}

    def _topic_buffer_full_dict(self) -> MutableMapping[str, int]:
        return {label(topic): count
                for topic, count in self.topic_buffer_full.items()}

    def _metric_counts_dict(self) -> MutableMapping[str, int]:
        return {key: count for key, count in self.metric_counts.items()}

    def _cleanup(self) -> None:
        self._cleanup_max_avg_history()
        self._cleanup_commit_latency_history()
        self._cleanup_send_latency_history()

    def _cleanup_max_avg_history(self) -> None:
        max_history = self.max_avg_history
        events_runtime = self.events_runtime
        if max_history is not None and len(events_runtime) > max_history:
            events_runtime[:len(events_runtime) - max_history] = []

    def _cleanup_commit_latency_history(self) -> None:
        max_history = self.max_commit_latency_history
        commit_latency = self.commit_latency
        if max_history is not None and len(commit_latency) > max_history:
            commit_latency[:len(commit_latency) - max_history] = []

    def _cleanup_send_latency_history(self) -> None:
        max_history = self.max_send_latency_history
        send_latency = self.send_latency
        if max_history is not None and len(send_latency) > max_history:
            send_latency[:len(send_latency) - max_history] = []

    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        # WARNING: Sensors must never keep a reference to the Message,
        #          as this means the message won't go out of scope!
        self.messages_received_total += 1
        self.messages_active += 1
        self.messages_received_by_topic[tp.topic] += 1
        message.time_in = self.time()

    def on_stream_event_in(self, tp: TP, offset: int, stream: StreamT,
                           event: EventT) -> None:
        self.events_total += 1
        self.events_by_stream[stream] += 1
        self.events_by_task[stream.task_owner] += 1
        self.events_active += 1
        event.message.stream_meta[id(stream)] = {
            'time_in': self.time(),
            'time_out': None,
            'time_total': None,
        }

    def on_stream_event_out(self, tp: TP, offset: int, stream: StreamT,
                            event: EventT) -> None:
        time_out = self.time()
        state = event.message.stream_meta[id(stream)]
        time_in = state['time_in']
        time_total = time_out - time_in
        self.events_active -= 1
        state.update(
            time_out=time_out,
            time_total=time_total,
        )
        self.events_runtime.append(time_total)

    def on_topic_buffer_full(self, topic: TopicT) -> None:
        self.topic_buffer_full[topic] += 1

    def on_message_out(self,
                       tp: TP,
                       offset: int,
                       message: Message) -> None:
        self.messages_active -= 1
        time_out = message.time_out = self.time()
        time_in = message.time_in
        if time_in is not None:
            message.time_total = time_out - time_in

    def on_table_get(self, table: CollectionT, key: Any) -> None:
        self._table_or_create(table).keys_retrieved += 1

    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        self._table_or_create(table).keys_updated += 1

    def on_table_del(self, table: CollectionT, key: Any) -> None:
        self._table_or_create(table).keys_deleted += 1

    def _table_or_create(self, table: CollectionT) -> TableState:
        try:
            return self.tables[table.name]
        except KeyError:
            state = self.tables[table.name] = TableState(table)
            return state

    def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        return self.time()

    def on_commit_completed(self, consumer: ConsumerT, state: Any) -> None:
        self.commit_latency.append(self.time() - cast(float, state))

    def on_send_initiated(self, producer: ProducerT, topic: str,
                          keysize: int, valsize: int) -> Any:
        self.messages_sent += 1
        self.messages_sent_by_topic[topic] += 1
        return self.time()

    def on_send_completed(self, producer: ProducerT, state: Any) -> None:
        self.send_latency.append(self.time() - cast(float, state))

    def count(self, metric_name: str, count: int = 1) -> None:
        self.metric_counts[metric_name] += count

    @cached_property
    def _service(self) -> ServiceT:
        return MonitorService(self)


class MonitorService(Service):
    """Service responsible for starting/stopping a sensor."""

    # Users may pass custom monitor to app, for example::
    #     app = faust.App(monitor=StatsdMonitor(prefix='word-count'))

    # When they do it's important to remember that the app is created during
    # module import, and that Service.__init__ creates the asyncio event loop.
    # To stop that from happening we use ServiceProxy to split this
    # into Monitor/MonitorService so that instantiating Monitor will not create
    # the service, instead the service is created lazily when first needed.

    def __init__(self, monitor: Monitor, **kwargs: Any) -> None:
        self.monitor: Monitor = monitor
        super().__init__()

    @Service.task
    async def _sampler(self) -> None:
        monitor = self.monitor
        median = statistics.median
        prev_message_total = monitor.messages_received_total
        prev_event_total = monitor.events_total
        while not self.should_stop:
            await self.sleep(1.0)

            # Update average event runtime.
            if monitor.events_runtime:
                monitor.events_runtime_avg = median(monitor.events_runtime)

            # Update events/s
            monitor.events_s, prev_event_total = (
                monitor.events_total - prev_event_total,
                monitor.events_total,
            )

            # Update messages/s
            monitor.messages_s, prev_message_total = (
                monitor.messages_received_total - prev_message_total,
                monitor.messages_received_total)

            # Cleanup
            monitor._cleanup()
