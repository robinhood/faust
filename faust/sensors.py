import asyncio
import typing
from time import monotonic
from typing import (
    Any, Counter, Iterator, List, Mapping, MutableMapping, Set, Tuple, cast,
)
from weakref import WeakValueDictionary
from .types import AppT, EventT, Message, StreamT, TableT, TopicPartition
from .types.sensors import SensorT, SensorDelegateT
from .types.transports import ConsumerT, ProducerT
from .utils.graphs.formatter import _label
from .utils.objects import KeywordReduce
from .utils.services import Service

__all__ = [
    'TableState',
    'EventState',
    'MessageState',
    'Sensor',
    'Monitor',
    'SensorDelegate',
]

MAX_MESSAGES = 1_000_000
MAX_AVG_HISTORY = 100
MAX_COMMIT_LATENCY_HISTORY = 30
MAX_SEND_LATENCY_HISTORY = 30


class TableState(KeywordReduce):

    #: The table this object records statistics for.
    table: TableT = None

    #: Number of times a key has been retrieved from this table.
    keys_retrieved: int = 0

    #: Number of times a key has been created/changed in this table.
    keys_updated: int = 0

    #: Number of times a key has been deleted from this table.
    keys_deleted: int = 0

    def __init__(self,
                 table: TableT,
                 *,
                 keys_retrieved: int = 0,
                 keys_updated: int = 0,
                 keys_deleted: int = 0) -> None:
        self.table: TableT = table
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


class EventState(KeywordReduce):

    #: The stream that received this event.
    stream: StreamT = None

    #: Monotonic timestamp of when the stream received this event.
    time_in: float = 0.0

    #: Monotonic timestamp of when the stream acknowledged this event.
    time_out: float = None

    #: Total event processing time (in seconds),
    #: or None if event is still processing.
    time_total: float = None

    def __init__(self,
                 stream: StreamT,
                 *,
                 time_in: float = None,
                 time_out: float = None,
                 time_total: float = None) -> None:
        self.stream = stream
        self.time_in = time_in
        self.time_out = time_out
        self.time_total = time_total

    def on_out(self) -> None:
        self.time_out = monotonic()
        self.time_total = self.time_out - self.time_in

    def asdict(self) -> Mapping:
        return {
            'time_in': self.time_in,
            'time_out': self.time_out,
            'time_total': self.time_total,
        }

    def __reduce_keywords__(self) -> Mapping:
        return {**self.asdict(), 'stream': self.stream}


class MessageState(KeywordReduce):

    #: ID of the consumer that received this message.
    consumer_id: int = None

    #: The topic+partition this message was delivered to.
    tp: TopicPartition = None

    #: The offset of this message.
    offset: int = None

    #: Monotonic timestamp of when the consumer received this message.
    time_in: float = None

    #: Monotonic timestamp of when the consumer acknowledged this message.
    time_out: float = None

    #: Total processing time (in seconds), or None if the event is
    #: still processing.
    time_total: float = None

    #: Every stream that receives this message will get an EventState instance
    #: in this list.
    streams: List[EventState] = None

    #: Fast index from stream to EventState.
    stream_index: MutableMapping[StreamT, EventState] = None

    def __init__(self,
                 consumer_id: int = None,
                 tp: TopicPartition = None,
                 offset: int = None,
                 *,
                 time_in: float = None,
                 time_out: float = None,
                 time_total: float = None,
                 streams: List[EventState] = None) -> None:
        self.consumer_id = consumer_id
        self.tp = tp
        self.offset = offset
        self.time_in = time_in
        self.time_out = time_out
        self.time_total = time_total
        self.streams = []
        self.stream_index = {
            ev.stream: ev for ev in self.streams
        }

    def asdict(self) -> Mapping:
        return {
            'consumer_id': self.consumer_id,
            'topic': self.tp.topic,
            'partition': self.tp.partition,
            'time_in': self.time_in,
            'time_out': self.time_out,
            'time_total': self.time_total,
            'streams': [s.asdict() for s in self.streams],
        }

    def __reduce_keywords__(self) -> Mapping:
        return {**self.asdict(), 'streams': self.streams}

    def on_stream_in(self, stream: StreamT, event: EventT) -> None:
        ev = EventState(stream, time_in=monotonic())
        self.streams.append(ev)
        self.stream_index[stream] = ev

    def on_stream_out(self, stream: StreamT, event: EventT) -> EventState:
        s = self.stream_index[stream]
        s.on_out()
        return s

    def on_out(self) -> None:
        self.time_out = monotonic()
        self.time_total = self.time_out - self.time_in


class Sensor(SensorT, Service):
    """Base class for sensors.

    This sensor does not do anything at all, but can be subclassed
    to create new sensors.
    """

    async def on_message_in(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message) -> None:
        """Called whenever a message is received by a consumer."""
        # WARNING: Sensors must never keep a reference to the Message,
        #          as this means the message won't go out of scope!
        ...

    async def on_stream_event_in(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        """Called whenever a message is sent to a stream as an event."""
        ...

    async def on_stream_event_out(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        """Called whenever an event is acknowledged (finished processing)."""
        ...

    async def on_message_out(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message = None) -> None:
        """Called when all streams have processed the message."""
        ...

    def on_table_get(self, table: TableT, key: Any) -> None:
        """Called whenever a key is retrieved from a table."""
        ...

    def on_table_set(self, table: TableT, key: Any, value: Any) -> None:
        """Called whenever a key is updated in a table."""
        ...

    def on_table_del(self, table: TableT, key: Any) -> None:
        """Called whenever a key is deleted from a table."""
        ...

    async def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        """Called when a consumer is about to commit the offset."""
        ...

    async def on_commit_completed(
            self, consumer: ConsumerT, state: Any) -> None:
        """Called after the offset is committed."""
        ...

    async def on_send_initiated(
            self, producer: ProducerT, topic: str,
            keysize: int, valsize: int) -> Any:
        """Called when a producer is about to send a message."""
        ...

    async def on_send_completed(
            self, producer: ProducerT, state: Any) -> None:
        """Called after a producer has sent a message."""
        ...


class Monitor(Sensor, KeywordReduce):
    """Default Faust Sensor.

    This is the default sensor, recording statistics about
    events, etc.
    """

    #: Max number of messages to keep history about in memory.
    max_messages: int = 0

    #: Max number of total runtimes to keep to build average.
    max_avg_history: int = 0

    #: Max number of commit latency numbers to keep.
    max_commit_latency_history: int = 0

    #: Max number of send latency numbers to keep.
    max_send_latency_history: int = 0

    #: Mapping of tables
    tables: MutableMapping[str, TableState] = None

    #: List of messages, as :class:`MessageState` objects.
    #: Note that at most :attr:`max_messages` will be kept in memory.
    messages: List[MessageState] = None

    #: Number of messages currently being processed.
    messages_active: int = 0

    #: Number of messages processed in total.
    messages_received_total: int = 0

    #: Count of messages received by topic
    messages_received_by_topic: Counter[str] = None

    #: Number of messages being processed this second.
    messages_s: int = 0

    #: Number of messages sent in total.
    messages_sent: int = 0

    #: Number of messages sent by topic.
    messages_sent_by_topic: Counter[str] = None

    #: Number of events currently being processed.
    events_active: int = 0

    #: Number of events processed in total.
    events_total: int = 0

    #: Number of events being processed this second.
    events_s: int = 0

    #: Count of events processed by stream
    events_by_stream: Counter[str] = None

    #: Count of events processed by task
    events_by_task: Counter[str] = None

    #: Average event runtime over the last second.
    events_runtime_avg: float = None

    #: List of runtimes used for averages
    events_runtime: List[float] = None

    #: List of commit latency values
    commit_latency: List[float] = None

    #: List of send latency values
    send_latency: List[float] = None

    #: Index of [tp][offset] -> MessageState.
    if typing.TYPE_CHECKING:
        message_index: WeakValueDictionary[Tuple[TopicPartition, int],
                                           MessageState]

    def __init__(self,
                 *,
                 max_messages: int = MAX_MESSAGES,
                 max_avg_history: int = MAX_AVG_HISTORY,
                 max_commit_latency_history: int = MAX_COMMIT_LATENCY_HISTORY,
                 max_send_latency_history: int = MAX_SEND_LATENCY_HISTORY,
                 messages: List[MessageState] = None,
                 messages_sent: int = 0,
                 tables: MutableMapping[str, TableState] = None,
                 messages_active: int = 0,
                 events_active: int = 0,
                 messages_received_total: int = 0,
                 events_total: int = 0,
                 events_by_stream: Counter[StreamT] = None,
                 events_by_task: Counter[asyncio.Task] = None,
                 events_runtime: List[float] = None,
                 commit_latency: List[float] = None,
                 send_latency: List[float] = None,
                 events_s: int = 0,
                 messages_s: int = 0,
                 events_runtime_avg: float = 0.0,
                 **kwargs: Any) -> None:
        self.max_messages = max_messages
        self.max_avg_history = max_avg_history
        self.max_commit_latency_history = max_commit_latency_history
        self.max_send_latency_history = max_send_latency_history

        self.messages = [] if messages is None else messages
        self.message_index = WeakValueDictionary()
        self.message_index.update({
            (e.tp, e.offset): e for e in self.messages
        })
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
        Service.__init__(self, **kwargs)

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
            'events_by_task': self.events_by_task,
            'events_by_stream': self.events_by_stream,
            'commit_latency': self.commit_latency,
            'send_latency': self.send_latency,
            'tables': {
                name: table.asdict() for name, table in self.tables.items()
            }
        }

    async def on_start(self) -> None:
        self.add_future(self._sampler())

    async def _sampler(self) -> None:
        prev_message_total = self.messages_received_total
        prev_event_total = self.events_total
        while not self.should_stop:
            await asyncio.sleep(1.0, loop=self.loop)

            # Update average event runtime.
            if self.events_runtime:
                self.events_runtime_avg = (
                    sum(self.events_runtime) / len(self.events_runtime))

            # Update events/s
            self.events_s = self.events_total - prev_event_total
            prev_event_total = self.events_total

            # Update messages/s
            self.messages_s = self.messages_received_total - prev_message_total
            prev_message_total = self.messages_received_total

            # Cleanup
            self._cleanup()

    def _cleanup(self) -> None:
        max_messages = self.max_messages
        if max_messages is not None and len(self.messages) > max_messages:
            self.messages[:len(self.messages) - max_messages] = []

        max_avg = self.max_avg_history
        if max_avg is not None and len(self.events_runtime) > max_avg:
            self.events_runtime[:len(self.events_runtime) - max_avg] = []

        max_com = self.max_commit_latency_history
        if max_com is not None and len(self.commit_latency) > max_com:
            self.commit_latency[:len(self.commit_latency) - max_com] = []

        max_sen = self.max_send_latency_history
        if max_sen is not None and len(self.send_latency) > max_sen:
            self.send_latency[:len(self.send_latency) - max_sen] = []

    async def on_message_in(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message) -> None:
        # WARNING: Sensors must never keep a reference to the Message,
        #          as this means the message won't go out of scope!
        self.messages_received_total += 1
        self.messages_active += 1
        self.messages_received_by_topic[tp.topic] += 1
        state = MessageState(consumer_id, tp, offset, time_in=monotonic())
        self.messages.append(state)
        self.message_index[(tp, offset)] = state

    async def on_stream_event_in(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        self.events_total += 1
        self.events_by_stream[_label(stream)] += 1
        self.events_by_task[_label(stream.task_owner)] += 1
        self.events_active += 1
        self.message_index[(tp, offset)].on_stream_in(stream, event)

    async def on_stream_event_out(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        self.events_active -= 1
        state = self.message_index[(tp, offset)].on_stream_out(stream, event)
        self.events_runtime.append(state.time_total)

    async def on_message_out(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message = None) -> None:
        self.messages_active -= 1
        try:
            self.message_index[(tp, offset)].on_out()
        except KeyError:
            pass

    def on_table_get(self, table: TableT, key: Any) -> None:
        self._table_or_create(table).keys_retrieved += 1

    def on_table_set(self, table: TableT, key: Any, value: Any) -> None:
        self._table_or_create(table).keys_updated += 1

    def on_table_del(self, table: TableT, key: Any) -> None:
        self._table_or_create(table).keys_deleted += 1

    def _table_or_create(self, table: TableT) -> TableState:
        try:
            return self.tables[table.table_name]
        except KeyError:
            state = self.tables[table.table_name] = TableState(table)
            return state

    async def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        return monotonic()

    async def on_commit_completed(
            self, consumer: ConsumerT, state: Any) -> None:
        self.commit_latency.append(monotonic() - cast(float, state))

    async def on_send_initiated(
            self, producer: ProducerT, topic: str,
            keysize: int, valsize: int) -> Any:
        self.messages_sent += 1
        self.messages_sent_by_topic[topic] += 1
        return monotonic()

    async def on_send_completed(
            self, producer: ProducerT, state: Any) -> None:
        self.send_latency.append(monotonic() - cast(float, state))


class SensorDelegate(SensorDelegateT):

    _sensors: Set[SensorT]

    def __init__(self, app: AppT) -> None:
        self.app = app
        self._sensors = set()

    def add(self, sensor: SensorT) -> None:
        # connect beacons
        sensor.beacon = self.app.beacon.new(sensor)
        self._sensors.add(sensor)

    def remove(self, sensor: SensorT) -> None:
        self._sensors.remove(sensor)

    def __iter__(self) -> Iterator:
        return iter(self._sensors)

    async def on_message_in(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message) -> None:
        for sensor in self._sensors:
            await sensor.on_message_in(consumer_id, tp, offset, message)

    async def on_stream_event_in(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        for sensor in self._sensors:
            await sensor.on_stream_event_in(tp, offset, stream, event)

    async def on_stream_event_out(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        for sensor in self._sensors:
            await sensor.on_stream_event_out(tp, offset, stream, event)

    async def on_message_out(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message = None) -> None:
        for sensor in self._sensors:
            await sensor.on_message_out(consumer_id, tp, offset, message)

    def on_table_get(self, table: TableT, key: Any) -> None:
        for sensor in self._sensors:
            sensor.on_table_get(table, key)

    def on_table_set(self, table: TableT, key: Any, value: Any) -> None:
        for sensor in self._sensors:
            sensor.on_table_set(table, key, value)

    def on_table_del(self, table: TableT, key: Any) -> None:
        for sensor in self._sensors:
            sensor.on_table_del(table, key)

    async def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        # This returns arbitrary state, so we return a map from sensor->state.
        return {
            sensor: await sensor.on_commit_initiated(consumer)
            for sensor in self._sensors
        }

    async def on_commit_completed(
            self, consumer: ConsumerT, state: Any) -> None:
        # state is now a mapping from sensor->state, so
        # make sure to correct the correct state to each sensor.
        for sensor in self._sensors:
            await sensor.on_commit_completed(consumer, state[sensor])

    async def on_send_initiated(
            self, producer: ProducerT, topic: str,
            keysize: int, valsize: int) -> Any:
        return {
            sensor: await sensor.on_send_initiated(
                producer, topic, keysize, valsize)
            for sensor in self._sensors
        }

    async def on_send_completed(self, producer: ProducerT, state: Any) -> None:
        for sensor in self._sensors:
            await sensor.on_send_completed(producer, state[sensor])
