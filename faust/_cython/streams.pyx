# cython: language_level=3
from asyncio import sleep
from time import monotonic
from mode.utils.futures import maybe_async, notify
from faust.exceptions import Skip
from faust.types import ChannelT, EventT


cdef class StreamIterator:

    cdef public:
        object stream
        object channel
        bint chan_is_channel
        object chan_queue
        object chan_queue_empty
        object chan_errors
        object chan_quick_get
        object chan_slow_get
        object processors
        object loop
        object on_merge
        object on_stream_event_in
        object on_stream_event_out
        object on_message_in
        object on_message_out
        object acking_topics
        object consumer
        object unacked
        object add_unacked
        object app
        object topics
        object acks_enabled_for
        object _skipped_value

    def __init__(self, object stream):
        self.stream = stream
        self.channel = self.stream.channel
        self.app = self.stream.app
        self.topics = self.app.topics
        self.acks_enabled_for = self.topics.acks_enabled_for
        self.loop = self.stream.loop
        self.on_merge = self.stream.on_merge
        self.on_stream_event_in = self.stream._on_stream_event_in
        self.on_stream_event_out = self.stream._on_stream_event_out
        self.on_message_in = self.stream._on_message_in
        self.on_message_out = self.stream._on_message_out
        self.acking_topics = stream.app.topics._acking_topics
        self.consumer = self.stream.app.consumer
        self.unacked = self.consumer._unacked_messages
        self.add_unacked = self.unacked.add
        self._skipped_value = self.stream._skipped_value

        if isinstance(self.channel, ChannelT):
            self.chan_is_channel = True
            self.chan_queue = self.channel.queue
            self.chan_queue_empty = self.chan_queue.empty
            self.chan_errors = self.chan_queue._errors
            self.chan_quick_get = self.chan_queue.get_nowait
        else:
            self.chan_is_channel = False
            self.chan_queue = None
            self.chan_queue_empty = None
            self.chan_errors = None
            self.chan_quick_get = None
        self.chan_slow_get = self.channel.__anext__
        self.processors = self.stream._processors

    async def next(self):
        cdef:
            object event
            object value
            object channel_value
            object stream
            object sensor_state
            bint enable_acks
        sensor_state = None
        stream = self.stream
        do_ack = stream.enable_acks

        value = None

        while value is None:
            await sleep(0, loop=self.loop)
            need_slow_get, channel_value = self._try_get_quick_value()
            if need_slow_get:
                channel_value = await self.chan_slow_get()
            value, sensor_state = self._prepare_event(channel_value)

            try:
                for processor in self.processors:
                    value = await maybe_async(processor(value))
                value = await self.on_merge(value)
            except Skip:
                value = self._skipped_value
        return value, sensor_state

    cpdef object after(self, object event, object do_ack, object sensor_state):
        cdef:
            bint last_stream_to_ack
            int refcount
            object tp
            object offset
            object consumer
        consumer = self.consumer
        last_stream_to_ack = False
        if do_ack and event is not None:
            message = event.message
            if not message.acked:
                refcount = message.refcount
                refcount -= 1
                if refcount < 0:
                    refcount = 0
                message.refcount = refcount
                if not refcount:
                    message.acked = True
                    tp = message.tp
                    offset = message.offset
                    if self.acks_enabled_for(message.topic):
                        committed = consumer._committed_offset[tp]
                        try:
                            if committed is None or offset > committed:
                                acked_index = consumer._acked_index[tp]
                                if offset not in acked_index:
                                    self.unacked.discard(message)
                                    acked_index.add(offset)
                                    acked_for_tp = consumer._acked[tp]
                                    acked_for_tp.append(offset)
                                    consumer._n_acked += 1
                                    last_stream_to_ack = True
                        finally:
                            notify(consumer._waiting_for_ack)
            tp = event.message.tp
            offset = event.message.offset
            self.on_stream_event_out(
                tp, offset, self.stream, event, sensor_state)
            if last_stream_to_ack:
                self.on_message_out(tp, offset, message)

    cdef object _prepare_event(self, object channel_value):
        cdef:
            object event
            object message
            object topic
            object tp
            object offset
            object consumer
            object stream_state
        stream_state = None
        if isinstance(channel_value, EventT):
            event = channel_value
            message = event.message
            topic = message.topic
            tp = message.tp
            offset = message.offset
            consumer = self.consumer

            if topic in self.acking_topics and not message.tracked:
                message.tracked = True
                self.add_unacked(message)
                self.on_message_in(tp, offset, message)

                stream_state = self.on_stream_event_in(
                    tp, offset, self.stream, event)
            self.stream._set_current_event(event)
            return (event.value, stream_state)
        else:
            self.stream._set_current_event(None)
            return channel_value, stream_state

    cdef object _try_get_quick_value(self):
        if self.chan_is_channel:
            if self.chan_errors:
                raise self.chan_errors.popleft()
            if self.chan_queue_empty:
                return (True, None)
            else:
                return self.chan_quick_get()
        return (True, None)
