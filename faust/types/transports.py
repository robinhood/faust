# App.transport creates Kafka consumer and producer.
import abc
import asyncio
import typing
from typing import (
    AbstractSet,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    Iterable,
    Mapping,
    MutableMapping,
    MutableSet,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    no_type_check,
)

from mode import Seconds, ServiceT
from yarl import URL

from .channels import ChannelT
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
    'ConductorT',
    'TransportT',
]

#: Callback called by :class:`faust.transport.base.Consumer` whenever
#: a message is received.
ConsumerCallback = Callable[[Message], Awaitable]

#: Argument to Consumer.commit to specify topics/tps to commit.
TPorTopic = Union[str, TP]
TPorTopicSet = AbstractSet[TPorTopic]

#: Callback (:keyword:`async def`) called when consumer partitions are revoked.
PartitionsRevokedCallback = Callable[[Set[TP]], Awaitable[None]]

#: Callback (:keyword:`async def`) called when consumer
#: partitions are assigned.
PartitionsAssignedCallback = Callable[[Set[TP]], Awaitable[None]]


class ConsumerT(ServiceT):

    #: The transport that created this Consumer.
    transport: 'TransportT'

    #: How often we commit topic offsets.
    #: See :setting:`broker_commit_interval`.
    commit_interval: float

    #: Set of topic names that are considered "randomly assigned".
    #: This means we don't crash if it's not part of our assignment.
    #: Used by e.g. the leader assignor service.
    randomly_assigned_topics: Set[str]

    @abc.abstractmethod
    def __init__(self,
                 transport: 'TransportT',
                 callback: ConsumerCallback,
                 on_partitions_revoked: PartitionsRevokedCallback,
                 on_partitions_assigned: PartitionsAssignedCallback,
                 *,
                 commit_interval: float = None,
                 loop: asyncio.AbstractEventLoop = None,
                 **kwargs: Any) -> None:
        self._on_partitions_revoked: PartitionsRevokedCallback
        self._on_partitions_assigned: PartitionsAssignedCallback

    @abc.abstractmethod
    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
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
    async def getmany(self,
                      timeout: float) -> AsyncIterator[Tuple[TP, Message]]:
        ...

    @abc.abstractmethod
    def track_message(self, message: Message) -> None:
        ...

    @abc.abstractmethod
    async def perform_seek(self) -> None:
        ...

    @abc.abstractmethod
    def ack(self, message: Message) -> bool:
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
    async def pause_partitions(self, tps: Iterable[TP]) -> None:
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

    @abc.abstractmethod
    def close(self) -> None:
        ...

    @property
    @abc.abstractmethod
    def unacked(self) -> Set[Message]:
        ...


class ProducerT(ServiceT):

    #: The transport that created this Producer.
    transport: 'TransportT'
    linger_ms: int
    max_batch_size: int

    @abc.abstractmethod
    def __init__(self, transport: 'TransportT',
                 loop: asyncio.AbstractEventLoop = None,
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int]) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int]) -> RecordMetadata:
        ...

    @abc.abstractmethod
    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
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


class ConductorT(ServiceT, MutableSet[ChannelT]):

    # The topic conductor delegates messages from the Consumer
    # to the various Topic instances subscribed to a topic.

    app: AppT

    @abc.abstractmethod
    def __init__(self, app: AppT, **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def acks_enabled_for(self, topic: str) -> bool:
        ...

    @abc.abstractmethod
    async def commit(self, topics: TPorTopicSet) -> bool:
        ...

    @abc.abstractmethod
    async def wait_for_subscriptions(self) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        ...


class TransportT(abc.ABC):

    #: The Consumer class used for this type of transport.
    Consumer: ClassVar[Type[ConsumerT]]

    #: The Producer class used for this type of transport.
    Producer: ClassVar[Type[ProducerT]]

    #: The Conductor class used to delegate messages from Consumer to streams.
    Conductor: ClassVar[Type[ConductorT]]

    #: The Fetcher service used for this type of transport.
    Fetcher: ClassVar[Type[ServiceT]]

    #: The :class:`faust.App` that created this transport.
    app: AppT

    #: The URL to use for this transport (e.g. kafka://localhost).
    url: URL

    #: String identifying the underlying driver used for this transport.
    #: E.g. for :pypi:`aiokafka` this could be "aiokafka 0.4.1".
    driver_version: str

    loop: asyncio.AbstractEventLoop

    @abc.abstractmethod
    def __init__(self,
                 url: Union[str, URL],
                 app: AppT,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    @abc.abstractmethod
    def create_consumer(self, callback: ConsumerCallback,
                        **kwargs: Any) -> ConsumerT:
        ...

    @abc.abstractmethod
    def create_producer(self, **kwargs: Any) -> ProducerT:
        ...

    @abc.abstractmethod
    def create_conductor(self, **kwargs: Any) -> ConductorT:
        ...
