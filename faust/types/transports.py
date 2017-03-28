import abc
import asyncio
import typing
from typing import Any, Callable, Awaitable, Optional, Type
from .core import K
from .models import Event
from .services import ServiceT
from .tuples import Message, Topic

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa

__all__ = [
    'ConsumerCallback',
    'KeyDecodeErrorCallback',
    'ValueDecodeErrorCallback',
    'EventRefT',
    'ConsumerT',
    'ProducerT',
    'TransportT',
]


#: Callback called by :class:`faust.transport.base.Consumer` whenever
#: a message is received.
ConsumerCallback = Callable[[Topic, K, Event], Awaitable]

#: Callback called by :class:`faust.transport.base.Consumer` whenever
#: a message key cannot be decoded/deserialized.
KeyDecodeErrorCallback = Callable[[Exception, Message], Awaitable]

#: Callback called by :class:`faust.transport.base.Consumer` whenever
#: a message value cannot be decoded/deserialized.
ValueDecodeErrorCallback = Callable[[Exception, Message], Awaitable]


class EventRefT(metaclass=abc.ABCMeta):
    consumer_id: int
    offset: int


class ConsumerT(ServiceT):

    id: int
    topic: Topic
    transport: 'TransportT'
    commit_interval: float

    @abc.abstractmethod
    async def _commit(self, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def register_timers(self) -> None:
        ...

    @abc.abstractmethod
    async def on_message(self, message: Message) -> None:
        ...

    @abc.abstractmethod
    def track_event(self, event: Event, offset: int) -> None:
        ...

    @abc.abstractmethod
    def on_event_ready(self, ref: EventRefT) -> None:
        ...


class ProducerT(ServiceT):
    transport: 'TransportT'

    @abc.abstractmethod
    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        ...

    @abc.abstractmethod
    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        ...


class TransportT(metaclass=abc.ABCMeta):
    Consumer: Type
    Producer: Type

    app: AppT
    url: str
    loop: asyncio.AbstractEventLoop

    def create_consumer(self, topic: Topic, callback: ConsumerCallback,
                        **kwargs: Any) -> ConsumerT:
        ...

    def create_producer(self, **kwargs: Any) -> ProducerT:
        ...
