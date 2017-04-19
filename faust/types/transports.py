import abc
import asyncio
import typing
from typing import Any, Callable, ClassVar, Awaitable, Optional, Type
from faust.utils.types.services import ServiceT
from .tuples import Message, Topic

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa

__all__ = [
    'ConsumerCallback',
    'MessageRefT',
    'ConsumerT',
    'ProducerT',
    'TransportT',
]


#: Callback called by :class:`faust.transport.base.Consumer` whenever
#: a message is received.
ConsumerCallback = Callable[[Message], Awaitable]


class MessageRefT(metaclass=abc.ABCMeta):
    consumer_id: int
    offset: int


class ConsumerT(ServiceT):

    id: int
    topic: Topic
    transport: 'TransportT'
    commit_interval: float

    @abc.abstractmethod
    async def subscribe(self, pattern: str) -> None:
        ...

    @abc.abstractmethod
    async def _commit(self, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def register_timers(self) -> None:
        ...

    @abc.abstractmethod
    async def on_task_error(self, exc: Exception) -> None:
        ...

    @abc.abstractmethod
    async def track_message(self, message: Message, offset: int) -> None:
        ...

    @abc.abstractmethod
    def on_message_ready(self, ref: MessageRefT) -> None:
        ...


class ProducerT(ServiceT):
    transport: 'TransportT'

    @abc.abstractmethod
    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes]) -> Awaitable:
        ...

    @abc.abstractmethod
    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes]) -> Awaitable:
        ...


class TransportT(metaclass=abc.ABCMeta):
    Consumer: ClassVar[Type]
    Producer: ClassVar[Type]

    app: AppT
    url: str
    loop: asyncio.AbstractEventLoop
    driver_version: str

    def create_consumer(self, callback: ConsumerCallback,
                        **kwargs: Any) -> ConsumerT:
        ...

    def create_producer(self, **kwargs: Any) -> ProducerT:
        ...
