import asyncio
import typing
from time import monotonic
from typing import Any, Counter, List, Mapping, MutableMapping, Tuple
from weakref import WeakValueDictionary
from .types import Event, Message, SensorT, StreamT, TableT, TopicPartition
from .utils.graphs.formatter import _label
from .utils.objects import KeywordReduce
from .utils.services import Service

MAX_MESSAGES = 1_000_000
MAX_AVG_HISTORY = 100


class TableState(KeywordReduce):

    table: TableT
    keys_retrieved: int
    keys_updated: int
    keys_deleted: int

    def __init__(self,
                 table: TableT,
                 *,
                 keys_retrieved: int = 0,
                 keys_updated: int = 0,
                 keys_deleted: int = 0) -> None:
        self.table = table
        self.keys_retrieved = keys_retrieved
        self.keys_updated = keys_updated
        self.keys_deleted = keys_deleted


class EventState(KeywordReduce):

    def __init__(self,
                 stream: StreamT,
                 *,
                 time_in: float = None,
                 time_out: float = None,
                 time_total: float = None) -> None:
        self.time_in: float = time_in
        self.time_out: float = time_out
        self.time_total: float = time_total

    def on_out(self) -> None:
        self.time_out = monotonic()
        self.time_total = self.time_out - self.time_in


class MessageState(KeywordReduce):

    consumer_id: int
    tp: TopicPartition
    offset: int
    time_in: float
    time_out: float
    time_total: float

    streams: List[EventState]
    stream_index: MutableMapping[StreamT, EventState]

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
        self.stream_index = {}

    def on_stream_in(self, stream: StreamT, event: Event) -> None:
        ev = EventState(stream, time_in=monotonic())
        self.streams.append(ev)
        self.stream_index[stream] = ev

    def on_stream_out(self, stream: StreamT, event: Event) -> EventState:
        s = self.stream_index[stream]
        s.on_out()
        return s

    def on_out(self) -> None:
        self.time_out = monotonic()
        self.time_total = self.time_out - self.time_in


class Sensor(SensorT, Service, KeywordReduce):

    #: Max number of messages to keep history about in memory.
    max_messages: int

    #: Max number of total runtimes to keep to build average.
    max_avg_history: int

    #: Number of messages currently being processed.
    active_messages: int

    #: Number of events currently being processed.
    active_events: int

    #: Number of messages processed in total.
    total_messages: int

    #: Number of events processed in total.
    total_events: int

    #: Count of events processed by stream
    total_by_stream: Counter[StreamT]

    #: Count of events processed by task
    total_by_task: Counter[str]

    #: Count of messages received by topic
    total_by_topic: Counter[str]

    #: List of runtimes used for averages
    event_runtimes: List[float]

    #: Number of events being processed this second.
    events_s: int

    #: Number of messages being processed this second.
    messages_s: int

    #: List of messages
    messages: List[MessageState]

    #: Mapping of tables
    tables: MutableMapping[str, TableState]

    #: Index of [tp][offset] -> MessageState.
    if typing.TYPE_CHECKING:
        message_index: WeakValueDictionary[Tuple[TopicPartition, int],
                                           MessageState]

    def __init__(self,
                 *,
                 max_messages: int = MAX_MESSAGES,
                 max_avg_history: int = MAX_AVG_HISTORY,
                 messages: List[MessageState] = None,
                 tables: MutableMapping[str, TableState] = None,
                 active_messages: int = 0,
                 active_events: int = 0,
                 total_messages: int = 0,
                 total_events: int = 0,
                 total_by_stream: Counter[StreamT] = None,
                 total_by_task: Counter[asyncio.Task] = None,
                 event_runtimes: List[float] = None,
                 events_s: int = 0,
                 messages_s: int = 0,
                 avg_event_runtime: float = 0.0,
                 **kwargs: Any) -> None:
        self.max_messages = max_messages
        self.max_avg_history = max_avg_history
        self.messages = [] if messages is None else messages
        self.message_index = WeakValueDictionary()
        self.message_index.update({
            (e.tp, e.offset): e for e in self.messages
        })
        self.tables = {} if tables is None else tables
        self.active_messages = active_messages
        self.active_events = active_events
        self.total_messages = total_messages
        self.total_events = total_events
        self.total_by_stream = Counter()
        self.total_by_task = Counter()
        self.total_by_topic = Counter()
        self.event_runtimes = [] if event_runtimes is None else event_runtimes
        self.events_s = events_s
        self.messages_s = messages_s
        self.avg_event_runtime = avg_event_runtime
        Service.__init__(self, **kwargs)

    def asdict(self) -> Mapping:
        return {
            'active_messages': self.active_messages,
            'active_events': self.active_events,
            'total_messages': self.total_messages,
            'total_events': self.total_events,
            'events_s': self.events_s,
            'messages_s': self.messages_s,
            'avg_event_runtime': self.avg_event_runtime,
            'total_by_task': self.total_by_task,
            'total_by_topic': self.total_by_topic,
            'tables': {
                name: table.asdict() for name, table in self.tables.items()
            }
        }

    async def on_start(self) -> None:
        self.add_future(self._sampler())

    async def _sampler(self) -> None:
        prev_message_total = self.total_messages
        prev_event_total = self.total_events
        while not self.should_stop:
            await asyncio.sleep(1.0, loop=self.loop)

            # Update average event runtime.
            if self.event_runtimes:
                self.avg_event_runtime = (
                    sum(self.event_runtimes) / len(self.event_runtimes))

            # Update events/s
            self.events_s = self.total_events - prev_event_total
            prev_event_total = self.total_events

            # Update messages/s
            self.messages_s = self.total_messages - prev_message_total
            prev_message_total = self.total_messages

            # Cleanup
            self._cleanup()

    def _cleanup(self) -> None:
        return
        max_messages = self.max_messages
        if max_messages is not None and len(self.messages) > max_messages:
            self.messages[:len(self.messages) - max_messages] = []

        max_avg = self.max_avg_history
        if max_avg is not None and len(self.event_runtimes) > max_avg:
            self.event_runtimes[:len(self.event_runtimes) - max_avg] = []

    async def on_message_in(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message) -> None:
        # WARNING: Sensors must never keep a reference to the Message,
        #          as this means the message won't go out of scope!
        self.total_messages += 1
        self.active_messages += 1
        self.total_by_topic[tp.topic] += 1
        state = MessageState(consumer_id, tp, offset, time_in=monotonic())
        self.messages.append(state)
        self.message_index[(tp, offset)] = state

    async def on_stream_event_in(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: Event) -> None:
        self.total_events += 1
        self.total_by_stream[stream] += 1
        self.total_by_task[_label(stream.task_owner)] += 1
        self.active_events += 1
        self.message_index[(tp, offset)].on_stream_in(stream, event)

    async def on_stream_event_out(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: Event) -> None:
        self.active_events -= 1
        state = self.message_index[(tp, offset)].on_stream_out(stream, event)
        self.event_runtimes.append(state.time_total)

    async def on_message_out(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message = None) -> None:
        self.active_messages -= 1
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
