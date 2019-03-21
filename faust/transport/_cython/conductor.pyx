# cython: language_level=3
from asyncio import ALL_COMPLETED, wait
from faust.exceptions import KeyDecodeError, ValueDecodeError


cdef class ConductorHandler:

    cdef public:
        object conductor
        object tp
        object channels
        object app
        object topic
        object partition
        object on_topic_buffer_full
        object acquire_flow_control
        object consumer

    def __init__(self, object conductor, object tp, object channels):
        self.conductor = conductor
        self.tp = tp
        self.channels = channels

        self.app = self.conductor.app
        self.topic, self.partition = self.tp
        self.on_topic_buffer_full = self.app.sensors.on_topic_buffer_full
        self.acquire_flow_control = self.app.flow_control.acquire
        self.consumer = self.app.consumer

    async def __call__(self, object message):
        cdef:
            Py_ssize_t channels_n
            object channels
            object event
            object keyid
            object dest_event
        await self.acquire_flow_control()
        channels = self.channels
        channels_n = len(channels)
        if channels_n:
            message.refcount += channels_n  # message.incref(n)
            event = None
            event_keyid = None

            delivered = set()
            full = []
            try:
                for chan in channels:
                    event, event_keyid = self._decode(event, chan, event_keyid)
                    if event is None:
                        event = await chan.decode(message, propagate=True)
                    if not self._put(event, chan, full):
                        continue
                    delivered.add(chan)
                if full:
                    for event, chan in full:
                        self.on_topic_buffer_full(chan)
                        await chan.put(event)
                        delivered.add(chan)
            except KeyDecodeError as exc:
                remaining = channels - delivered
                message.ack(self.consumer, n=len(remaining))
                for channel in remaining:
                    await channel.on_key_decode_error(exc, message)
                    delivered.add(channel)
            except ValueDecodeError as exc:
                remaining = channels - delivered
                message.ack(self.consumer, n=len(remaining))
                for channel in remaining:
                    await channel.on_value_decode_error(exc, message)
                    delivered.add(channel)

    cdef object _decode(self, object event, object channel, object event_keyid):
        keyid = channel.key_type, channel.value_type
        if event_keyid is None or event is None:
            return None, event_keyid
        if keyid == event_keyid:
            return event, keyid

    cdef bint _put(self,
                   object event,
                   object channel,
                   object full):
        cdef:
            object queue
        queue = channel.queue
        if queue.full():
            full.append((event, channel))
            return False
        else:
            queue.put_nowait(event)
            return True
