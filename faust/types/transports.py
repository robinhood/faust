import abc
import asyncio
import typing
from typing import (
    AbstractSet, Any, AsyncIterator, Awaitable, Callable, ClassVar, Iterable,
    Mapping, MutableMapping, Optional, Set, Tuple, Type, Union, no_type_check,
)
from mode import Seconds, ServiceT
from yarl import URL
from .tuples import Message, RecordMetadata, TP

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
TPorTopic = Union[str, TP]
TPorTopicSet = AbstractSet[TPorTopic]

PartitionsRevokedCallback = Callable[[Iterable[TP]], Awaitable]
PartitionsAssignedCallback = Callable[[Iterable[TP]], Awaitable]


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
            *partitions: TP,
            timeout: float) -> AsyncIterator[Tuple[TP, Message]]:
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
    def assignment(self) -> Set[TP]:
        ...

    @abc.abstractmethod
    def highwater(self, tp: TP) -> int:
        ...

    @abc.abstractmethod
    async def pause_topics(self, topics: Iterable[str]) -> None:
        ...

    @abc.abstractmethod
    async def pause_partitions(self, tps: Iterable[TP]) -> None:
        ...

    @abc.abstractmethod
    async def resume_topics(self, topics: Iterable[str]) -> None:
        ...

    @abc.abstractmethod
    async def resume_partitions(self, tps: Iterable[TP]) -> None:
        ...

    @abc.abstractmethod
    async def position(self, tp: TP) -> Optional[int]:
        ...

    @abc.abstractmethod
    async def seek(self, partition: TP, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def commit(self, topics: TPorTopicSet = None) -> bool:
        ...

    @abc.abstractmethod
    async def on_task_error(self, exc: BaseException) -> None:
        ...

    @abc.abstractmethod
    async def earliest_offsets(self,
                               *partitions: TP) -> MutableMapping[TP, int]:
        ...

    @abc.abstractmethod
    async def highwaters(self, *partitions: TP) -> MutableMapping[TP, int]:
        ...

    @property
    @abc.abstractmethod
    def unacked(self) -> Set[Message]:
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
    def key_partition(self, topic: str, key: bytes) -> TP:
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
