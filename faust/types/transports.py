import abc
import asyncio
import typing
from typing import (
    AbstractSet, Any, AsyncIterator, Awaitable, Callable, ClassVar,
    Iterable, Mapping, Optional, Set, Tuple, Type, Union, no_type_check,
)
from yarl import URL
from .tuples import Message, RecordMetadata, TopicPartition
from ..utils.times import Seconds
from ..utils.types.services import ServiceT

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
TPorTopic = Union[str, TopicPartition]
TPorTopicSet = AbstractSet[TPorTopic]

PartitionsRevokedCallback = Callable[[Iterable[TopicPartition]], Awaitable]
PartitionsAssignedCallback = Callable[[Iterable[TopicPartition]], Awaitable]


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
    async def subscribe(self, topics: Iterable[str]) -> None:
        ...

    @abc.abstractmethod
    @no_type_check
    async def getmany(
            self,
            *partitions: TopicPartition,
            timeout: float) -> AsyncIterator[Tuple[TopicPartition, Message]]:
        ...

    @abc.abstractmethod
    async def track_message(self, message: Message) -> None:
        ...

    @abc.abstractmethod
    async def ack(self, message: Message) -> None:
        ...

    @abc.abstractmethod
    async def wait_empty(self) -> None:
        ...

    @abc.abstractmethod
    def assignment(self) -> Set[TopicPartition]:
        ...

    @abc.abstractmethod
    def highwater(self, tp: TopicPartition) -> int:
        ...

    @abc.abstractmethod
    async def pause_partitions(self, tps: Iterable[TopicPartition]) -> None:
        ...

    @abc.abstractmethod
    async def resume_partitions(self, tps: Iterable[TopicPartition]) -> None:
        ...

    @abc.abstractmethod
    async def position(self, tp: TopicPartition) -> Optional[int]:
        ...

    @abc.abstractmethod
    async def seek_to_latest(self, *partitions: TopicPartition) -> None:
        ...

    @abc.abstractmethod
    async def seek_to_beginning(self, *partitions: TopicPartition) -> None:
        ...

    @abc.abstractmethod
    async def seek(self, partition: TopicPartition, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def commit(self, topics: TPorTopicSet = None) -> bool:
        ...

    @abc.abstractmethod
    async def on_task_error(self, exc: Exception) -> None:
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
            partition: Optional[int]) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes],
            partition: Optional[int]) -> RecordMetadata:
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

    @abc.abstractmethod
    def key_partition(self, topic: str, key: bytes) -> TopicPartition:
        ...


class TransportT(abc.ABC):
    Consumer: ClassVar[Type[ConsumerT]]
    Producer: ClassVar[Type[ProducerT]]
    Fetcher: ClassVar[Type[ServiceT]]

    app: AppT
    url: URL
    loop: asyncio.AbstractEventLoop
    driver_version: str

    @abc.abstractmethod
    def __init__(self, url: Union[str, URL], app: AppT,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    @abc.abstractmethod
    def create_consumer(self, callback: ConsumerCallback,
                        **kwargs: Any) -> ConsumerT:
        ...

    @abc.abstractmethod
    def create_producer(self, **kwargs: Any) -> ProducerT:
        ...
