import asyncio
import faust
import weakref
from itertools import count
from typing import Awaitable, Callable, Optional, List, Tuple, cast
from ..event import Event
from ..exceptions import KeyDecodeError, ValueDecodeError
from ..types import (
    ConsumerCallback,
    K, KeyDecodeErrorCallback, V, ValueDecodeErrorCallback,
    Message, Topic,
)
from ..utils.serialization import loads
from ..utils.service import Service

CLIENT_ID = 'faust-{0}'.format(faust.__version__)


class EventRef(weakref.ref):

    def __init__(self, event: Event,
                 callback: Callable = None,
                 offset: int = None) -> None:
        super().__init__(event, callback)
        self.offset = offset


class Consumer(Service):
    id: int
    topic: Topic
    client_id = CLIENT_ID
    transport: 'Transport'

    commit_interval = 30.0

    #: This counter generates new consumer ids.
    _consumer_ids = count(0)

    _dirty_events: List[EventRef] = None
    _acked: List[int] = None
    _current_offset: int = None

    def __init__(self, transport: 'Transport',
                 *,
                 topic: Topic = None,
                 on_key_decode_error: KeyDecodeErrorCallback = None,
                 on_value_decode_error: ValueDecodeErrorCallback = None,
                 callback: ConsumerCallback = None) -> None:
        assert callback is not None
        self.id = next(self._consumer_ids)
        self.transport = transport
        self.callback = callback
        self.topic = topic
        self.type = self.topic.type
        self._key_serializer = self.topic.key_serializer
        self.on_key_decode_error = on_key_decode_error
        self.on_value_decode_error = on_value_decode_error
        if self.topic.topics and self.topic.pattern:
            raise TypeError('Topic can specify either topics or pattern')
        self._dirty_events = []
        self._acked = []
        super().__init__(loop=self.transport.loop)

    async def _commit(self, offset: int) -> None:
        raise NotImplementedError()

    async def on_message(self, message: Message) -> None:
        try:
            k, v = self.to_KV(message)
        except KeyDecodeError as exc:
            if not self.on_key_decode_error:
                raise
            await self.on_key_decode_error(exc, message)
        except ValueDecodeError as exc:
            if not self.on_value_decode_error:
                raise
            await self.on_value_decode_error(exc, message)
        self.track_event(v, message.offset)
        await self.callback(self.topic, k, v)

    def to_KV(self, message: Message) -> Tuple[K, V]:
        key = message.key
        if self._key_serializer:
            try:
                key = loads(self._key_serializer, message.key)
            except Exception as exc:
                raise KeyDecodeError(exc)
        k = cast(K, key)
        try:
            v = self.type.from_message(k, message)  # type: ignore
        except Exception as exc:
            raise ValueDecodeError(exc)
        return k, cast(V, v)

    def track_event(self, event: Event, offset: int) -> None:
        self._dirty_events.append(
            EventRef(event, self.on_event_ready, offset=offset))

    def on_event_ready(self, ref: EventRef) -> None:
        print('ACKED MESSAGE %r' % (ref.offset,))
        self._acked.append(ref.offset)
        self._acked.sort()

    async def register_timers(self) -> None:
        asyncio.ensure_future(self._commit_handler(), loop=self.loop)

    async def _commit_handler(self) -> None:
        asyncio.sleep(self.commit_interval)
        while 1:
            try:
                offset = self._new_offset()
            except IndexError:
                pass
            else:
                if self._should_commit(offset):
                    self._current_offset = offset
                    await self._commit(offset)
            await asyncio.sleep(self.commit_interval)

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


class Producer(Service):
    client_id = CLIENT_ID
    transport: 'Transport'

    def __init__(self, transport: 'Transport') -> None:
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


# We make aliases here, as mypy is confused by the class variables below.
_ProducerT = Producer
_ConsumerT = Consumer


class Transport:
    Consumer: type
    Producer: type

    url: str
    loop: asyncio.AbstractEventLoop

    def __init__(self,
                 url: str = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.url = url
        self.loop = loop

    def create_consumer(self, topic: Topic, callback: ConsumerCallback,
                        **kwargs) -> _ConsumerT:
        return cast(_ConsumerT, self.Consumer(
            self, topic=topic, callback=callback, **kwargs))

    def create_producer(self, **kwargs) -> _ProducerT:
        return cast(_ProducerT, self.Producer(self, **kwargs))
