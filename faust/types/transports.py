import abc
import asyncio
import typing
from typing import (
    AbstractSet, Any, Callable, ClassVar, Awaitable, Optional, Type, Union,
)
from faust.utils.types.services import ServiceT
from .tuples import Message, TopicPartition

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa

__all__ = [
    'ConsumerCallback',
    'TPorTopicSet',
    'ConsumerT',
    'ProducerT',
    'TransportT',
]


#: Callback called by :class:`faust.transport.base.Consumer` whenever
#: a message is received.
ConsumerCallback = Callable[[Message], Awaitable]

#: Argument to Consumer.commit to specify topics/tps to commit.
TPorTopicSet = AbstractSet[Union[str, TopicPartition]]


class ConsumerT(ServiceT):

    id: int
    transport: 'TransportT'
    commit_interval: float

    @abc.abstractmethod
    def ack(self, tp: TopicPartition, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def commit(self, topics: TPorTopicSet = None) -> bool:
        ...

    @abc.abstractmethod
    async def subscribe(self, pattern: str) -> None:
        ...

    @abc.abstractmethod
    async def _commit(self, offsets: Any) -> None:
        ...

    @abc.abstractmethod
    async def register_timers(self) -> None:
        ...

    @abc.abstractmethod
    async def on_task_error(self, exc: Exception) -> None:
        ...


class ProducerT(ServiceT):
    transport: 'TransportT'

    @abc.abstractmethod
    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes],
            partition: Optional[int]) -> Awaitable:
        ...

    @abc.abstractmethod
    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes],
            partition: Optional[int]) -> Awaitable:
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
