import abc
import asyncio
import typing
from typing import (
    AbstractSet, Any, Awaitable, Callable, ClassVar,
    Iterator, Mapping, Optional, Sequence, Tuple, Type, Union,
)
from faust.utils.times import Seconds
from faust.utils.types.services import ServiceT
from .tuples import Message, TopicPartition

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa

__all__ = [
    'ConsumerCallback',
    'TPorTopicSet',
    'PartitionsRevokedCallback',
    'PartitionsAssignedCallback',
    'ConsumerT',
    'ProducerT',
    'TransportT',
]


#: Callback called by :class:`faust.transport.base.Consumer` whenever
#: a message is received.
ConsumerCallback = Callable[[Message], Awaitable]

#: Argument to Consumer.commit to specify topics/tps to commit.
TPorTopicSet = AbstractSet[Union[str, TopicPartition]]

PartitionsRevokedCallback = Callable[[Sequence[TopicPartition]], None]
PartitionsAssignedCallback = Callable[[Sequence[TopicPartition]], None]


class ConsumerT(ServiceT):

    id: int
    transport: 'TransportT'
    commit_interval: float

    @abc.abstractmethod
    def __init__(self, transport: 'TransportT',
                 *,
                 callback: ConsumerCallback = None,
                 on_partitions_revoked: PartitionsRevokedCallback = None,
                 on_partitions_assigned: PartitionsAssignedCallback = None,
                 autoack: bool = True,
                 commit_interval: float = None,
                 **kwargs: Any) -> None:
        self._on_partitions_revoked: PartitionsRevokedCallback
        self._on_partitions_assigned: PartitionsAssignedCallback

    @abc.abstractmethod
    async def create_topic(self, topic: str, partitions: int, replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 1000.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        ...

    @abc.abstractmethod
    async def subscribe(self, pattern: str) -> None:
        ...

    @abc.abstractmethod
    async def getmany(
            self,
            *partitions: TopicPartition,
            timeout: float) -> Iterator[Tuple[TopicPartition, Message]]:
        ...

    @abc.abstractmethod
    def ack(self, tp: TopicPartition, offset: int) -> None:
        ...

    @abc.abstractmethod
    def pause_partitions(self, tps: Sequence[TopicPartition]) -> None:
        ...

    @abc.abstractmethod
    def resume_partitions(self, tps: Sequence[TopicPartition]) -> None:
        ...

    @abc.abstractmethod
    def reset_offset_earliest(self, topic_partiton: TopicPartition):
        ...

    @abc.abstractmethod
    async def commit(self, topics: TPorTopicSet = None) -> bool:
        ...

    @abc.abstractmethod
    async def on_task_error(self, exc: Exception) -> None:
        ...

    @abc.abstractmethod
    async def suspend(self) -> None:
        ...

    @abc.abstractmethod
    async def resume(self) -> None:
        ...


class ProducerT(ServiceT):
    transport: 'TransportT'

    @abc.abstractmethod
    def __init__(self, transport: 'TransportT', **kwargs: Any) -> None:
        ...

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

    @abc.abstractmethod
    async def create_topic(self, topic: str, partitions: int, replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 1000.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        ...


class TransportT(abc.ABC):
    Consumer: ClassVar[Type[ConsumerT]]
    Producer: ClassVar[Type[ProducerT]]

    app: AppT
    url: str
    loop: asyncio.AbstractEventLoop
    driver_version: str

    @abc.abstractmethod
    def create_consumer(self, callback: ConsumerCallback,
                        **kwargs: Any) -> ConsumerT:
        ...

    @abc.abstractmethod
    def create_producer(self, **kwargs: Any) -> ProducerT:
        ...
