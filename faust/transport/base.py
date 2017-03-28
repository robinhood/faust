"""Base message transport implementation."""
import asyncio
import weakref
from itertools import count
from typing import Any, Awaitable, Callable, Optional, List, Type, cast
from ..types import (
    AppT, ConsumerCallback, ConsumerT, Event, EventRefT,
    KeyDecodeErrorCallback, ValueDecodeErrorCallback,
    Message, ProducerT, Topic, TransportT,
)
from ..utils.services import Service

__all__ = ['EventRef', 'Consumer', 'Producer', 'Transport']

# The Transport is responsible for:
#
#  - Holds reference to the app that created it.
#  - Creates new consumers/producers.
#
# The Consumer is responsible for:
#
#   - Holds reference to the transport that created it
#   - ... and the app via ``self.transport.app``.
#   - Has a callback that usually points back to ``Stream.on_message``.
#   - Receives messages and calls the callback for every message received.
#   - The messages are deserialized first, so the Consumer also handles that.
#   - Keep track of the message and it's acked/unacked status.
#   - If automatic acks are enabled the message is acked when the Event goes
#     out of scope (like any variable using reference counting).
#   - Commits the offset at an interval
#      - The current offset is based on range of the messages acked.
#
# The Producer is responsible for:
#
#   - Holds reference to the transport that created it
#   - ... and the app via ``self.transport.app``.
#   - Sending messages.
#
# To see a reference transport implementation go to:
#     faust/transport/aiokafka.py


class EventRef(weakref.ref, EventRefT):
    """Weak-reference to :class:`ModelT`.

    Remembers the offset of the event, even after event out of scope.
    """

    # Used for tracking when events go out of scope.

    def __init__(self, event: Event,
                 callback: Callable = None,
                 offset: int = None,
                 consumer_id: int = None) -> None:
        super().__init__(event, callback)
        self.offset = offset
        self.consumer_id = consumer_id


class Consumer(ConsumerT, Service):
    """Base Consumer."""

    #: This counter generates new consumer ids.
    _consumer_ids = count(0)

    _app: AppT
    _dirty_events: List[EventRefT] = None
    _acked: List[int] = None
    _current_offset: int = None

    _commit_handler_fut: asyncio.Future = None

    def __init__(self, transport: TransportT,
                 *,
                 topic: Topic = None,
                 callback: ConsumerCallback = None,
                 autoack: bool = True,
                 on_key_decode_error: KeyDecodeErrorCallback = None,
                 on_value_decode_error: ValueDecodeErrorCallback = None,
                 commit_interval: float = None) -> None:
        assert callback is not None
        self.id = next(self._consumer_ids)
        self.transport = transport
        self._app = self.transport.app
        self.topic = topic
        self.callback = callback
        self.autoack = autoack
        self.key_type = self.topic.key_type
        self.value_type = self.topic.value_type
        self.on_key_decode_error = on_key_decode_error
        self.on_value_decode_error = on_value_decode_error
        self._loads_key = self._app.loads_key
        self._loads_value = self._app.loads_value
        self._on_event_in = self._app.on_event_in
        self._on_event_out = self._app.on_event_out
        self.commit_interval = (
            commit_interval or self._app.commit_interval)
        if self.topic.topics and self.topic.pattern:
            raise TypeError('Topic can specify either topics or pattern')
        self._dirty_events = []
        self._acked = []
        self._commit_mutex = asyncio.Lock(loop=self.loop)
        super().__init__(loop=self.transport.loop)

    async def register_timers(self) -> None:
        self._commit_handler_fut = asyncio.ensure_future(
            self._commit_handler(), loop=self.loop)

    async def on_message(self, message: Message) -> None:
        try:
            k = await self._loads_key(self.key_type, message.key)
        except Exception as exc:
            if not self.on_key_decode_error:
                raise
            await self.on_key_decode_error(exc, message)
        else:
            try:
                v = await self._loads_value(self.value_type, k, message)
            except Exception as exc:
                if not self.on_value_decode_error:
                    raise
                await self.on_value_decode_error(exc, message)
            else:
                self.track_event(v, message.offset)
                await self.callback(self.topic, k, v)

    def track_event(self, event: Event, offset: int) -> None:
        _id = self.id
        # keep weak reference to event, to be notified when out of scope.
        self._dirty_events.append(
            EventRef(event, self.on_event_ready,
                     offset=offset, consumer_id=self.id))
        # call sensors
        asyncio.ensure_future(
            self._on_event_in(_id, offset, event),
            loop=self.loop)

    def on_event_ready(self, ref: EventRefT) -> None:
        if self.autoack:
            self._acked.append(ref.offset)
            self._acked.sort()
        asyncio.ensure_future(
            self._on_event_out(ref.consumer_id, ref.offset, None),
            loop=self.loop)

    async def start(self) -> None:
        self._app.register_consumer(self)
        await super().start()

    async def _commit_handler(self) -> None:
        asyncio.sleep(self.commit_interval)
        while 1:
            await self.maybe_commit()
            await asyncio.sleep(self.commit_interval)

    async def maybe_commit(self) -> bool:
        async with self._commit_mutex:
            try:
                offset = self._new_offset()
            except IndexError:
                pass
            else:
                if self._should_commit(offset):
                    self._current_offset = offset
                    await self._commit(offset)
                    return True
        return False

    async def on_task_error(self, exc: Exception) -> None:
        if self.autoack:
            await self.maybe_commit()

    def _should_commit(self, offset) -> bool:
        return (
            self._current_offset is None or
            (offset and offset > self._current_offset)
        )

    def _new_offset(self) -> int:
        acked = self._acked
        for i, offset in enumerate(acked):
            if offset != acked[i - 1]:
                break
        else:
            raise IndexError()
        return offset


class Producer(ProducerT, Service):
    """Base Producer."""

    def __init__(self, transport: TransportT) -> None:
        self.transport = transport
        super().__init__(loop=self.transport.loop)

    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        raise NotImplementedError()

    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        raise NotImplementedError()


class Transport(TransportT):
    """Message transport implementation."""

    #: Consumer subclass used for this transport.
    Consumer: Type

    #: Producer subclass used for this transport.
    Producer: Type

    def __init__(self, url: str, app: AppT,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.url = url
        self.app = app
        self.loop = loop

    def create_consumer(self, topic: Topic, callback: ConsumerCallback,
                        **kwargs: Any) -> ConsumerT:
        return cast(ConsumerT, self.Consumer(
            self, topic=topic, callback=callback, **kwargs))

    def create_producer(self, **kwargs: Any) -> ProducerT:
        return cast(ProducerT, self.Producer(self, **kwargs))
