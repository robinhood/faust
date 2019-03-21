# App.transport creates Kafka consumer and producer.
import abc
import asyncio
import ssl
import typing
from typing import (
    AbstractSet,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableSet,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
    no_type_check,
)

from mode import Seconds, ServiceT
from yarl import URL

from .core import HeadersArg
from .channels import ChannelT
from .tuples import Message, RecordMetadata, TP

if typing.TYPE_CHECKING:
    from .app import AppT as _AppT
else:
    class _AppT: ...  # noqa

__all__ = [
    'ConsumerCallback',
    'TPorTopicSet',
    'PartitionsRevokedCallback',
    'PartitionsAssignedCallback',
    'PartitionerT',
    'ConsumerT',
    'ProducerT',
    'ConductorT',
    'TransactionManagerT',
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

PartitionerT = Callable[
    [Optional[bytes],   # key to hash (or None)
     Sequence[int],     # all partitions
     Sequence[int]],    # available partitions
    int,
]


class ProducerT(ServiceT):

    #: The transport that created this Producer.
    transport: 'TransportT'

    client_id: str
    linger_ms: int
    max_batch_size: int
    acks: int
    max_request_size: int
    compression_type: Optional[str]
    ssl_context: Optional[ssl.SSLContext]
    partitioner: Optional[PartitionerT]
    request_timeout: float

    @abc.abstractmethod
    def __init__(self, transport: 'TransportT',
                 loop: asyncio.AbstractEventLoop = None,
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int],
                   timestamp: Optional[float],
                   headers: Optional[HeadersArg],
                   *,
                   transactional_id: str = None) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int],
                            timestamp: Optional[float],
                            headers: Optional[HeadersArg],
                            *,
                            transactional_id: str = None) -> RecordMetadata:
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

    @abc.abstractmethod
    async def flush(self) -> None:
        ...

    @abc.abstractmethod
    async def begin_transaction(self, transactional_id: str) -> None:
        ...

    @abc.abstractmethod
    async def commit_transaction(self, transactional_id: str) -> None:
        ...

    @abc.abstractmethod
    async def abort_transaction(self, transactional_id: str) -> None:
        ...

    @abc.abstractmethod
    async def stop_transaction(self, transactional_id: str) -> None:
        ...

    @abc.abstractmethod
    async def maybe_begin_transaction(self, transactional_id: str) -> None:
        ...

    @abc.abstractmethod
    async def commit_transactions(
            self,
            tid_to_offset_map: Mapping[str, Mapping[TP, int]],
            group_id: str,
            start_new_transaction: bool = True) -> None:
        ...

    @abc.abstractmethod
    def supports_headers(self) -> bool:
        ...


class TransactionManagerT(ProducerT):
    consumer: 'ConsumerT'
    producer: 'ProducerT'

    @abc.abstractmethod
    def __init__(self,
                 transport: 'TransportT',
                 loop: asyncio.AbstractEventLoop = None,
                 *,
                 consumer: 'ConsumerT',
                 producer: 'ProducerT',
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        ...

    @abc.abstractmethod
    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        ...

    @abc.abstractmethod
    async def commit(self, offsets: Mapping[TP, int],
                     start_new_transaction: bool = True) -> bool:
        ...

    async def begin_transaction(self, transactional_id: str) -> None:
        raise NotImplementedError()

    async def commit_transaction(self, transactional_id: str) -> None:
        raise NotImplementedError()

    async def abort_transaction(self, transactional_id: str) -> None:
        raise NotImplementedError()

    async def stop_transaction(self, transactional_id: str) -> None:
        raise NotImplementedError()

    async def maybe_begin_transaction(self, transactional_id: str) -> None:
        raise NotImplementedError()

    async def commit_transactions(
            self,
            tid_to_offset_map: Mapping[str, Mapping[TP, int]],
            group_id: str,
            start_new_transaction: bool = True) -> None:
        raise NotImplementedError()


class SchedulingStrategyT:

    @abc.abstractmethod
    def __init__(self) -> None:
        ...

    @abc.abstractmethod
    def iterate(self, records: Mapping[TP, List]) -> Iterator[Tuple[TP, Any]]:
        ...


class ConsumerT(ServiceT):

    #: The transport that created this Consumer.
    transport: 'TransportT'

    transactions: TransactionManagerT

    #: How often we commit topic offsets.
    #: See :setting:`broker_commit_interval`.
    commit_interval: float

    #: Set of topic names that are considered "randomly assigned".
    #: This means we don't crash if it's not part of our assignment.
    #: Used by e.g. the leader assignor service.
    randomly_assigned_topics: Set[str]

    in_transaction: bool

    scheduler: SchedulingStrategyT

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
    def stop_flow(self) -> None:
        ...

    @abc.abstractmethod
    def resume_flow(self) -> None:
        ...

    @abc.abstractmethod
    def pause_partitions(self, tps: Iterable[TP]) -> None:
        ...

    @abc.abstractmethod
    def resume_partitions(self, tps: Iterable[TP]) -> None:
        ...

    @abc.abstractmethod
    async def position(self, tp: TP) -> Optional[int]:
        ...

    @abc.abstractmethod
    async def seek(self, partition: TP, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def seek_wait(self, partitions: Mapping[TP, int]) -> None:
        ...

    @abc.abstractmethod
    async def commit(self,
                     topics: TPorTopicSet = None,
                     start_new_transaction: bool = True) -> bool:
        ...

    @abc.abstractmethod
    async def on_task_error(self, exc: BaseException) -> None:
        ...

    @abc.abstractmethod
    async def earliest_offsets(self, *partitions: TP) -> Mapping[TP, int]:
        ...

    @abc.abstractmethod
    async def highwaters(self, *partitions: TP) -> Mapping[TP, int]:
        ...

    @abc.abstractmethod
    def topic_partitions(self, topic: str) -> Optional[int]:
        ...

    @abc.abstractmethod
    def key_partition(self,
                      topic: str,
                      key: Optional[bytes],
                      partition: int = None) -> int:
        ...

    @abc.abstractmethod
    def close(self) -> None:
        ...

    @property
    @abc.abstractmethod
    def unacked(self) -> Set[Message]:
        ...


class ConductorT(ServiceT, MutableSet[ChannelT]):

    # The topic conductor delegates messages from the Consumer
    # to the various Topic instances subscribed to a topic.

    app: _AppT

    @abc.abstractmethod
    def __init__(self, app: _AppT, **kwargs: Any) -> None:
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


class TransportT(abc.ABC):

    #: The Consumer class used for this type of transport.
    Consumer: ClassVar[Type[ConsumerT]]

    #: The Producer class used for this type of transport.
    Producer: ClassVar[Type[ProducerT]]

    #: The TransactionManager class used for managing multiple transactions.
    TransactionManager: ClassVar[Type[TransactionManagerT]]

    #: The Conductor class used to delegate messages from Consumer to streams.
    Conductor: ClassVar[Type[ConductorT]]

    #: The Fetcher service used for this type of transport.
    Fetcher: ClassVar[Type[ServiceT]]

    #: The :class:`faust.App` that created this transport.
    app: _AppT

    #: The URL to use for this transport (e.g. kafka://localhost).
    url: List[URL]

    #: String identifying the underlying driver used for this transport.
    #: E.g. for :pypi:`aiokafka` this could be "aiokafka 0.4.1".
    driver_version: str

    loop: asyncio.AbstractEventLoop

    @abc.abstractmethod
    def __init__(self,
                 url: List[URL],
                 app: _AppT,
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
    def create_transaction_manager(self,
                                   consumer: ConsumerT,
                                   producer: ProducerT,
                                   **kwargs: Any) -> TransactionManagerT:
        ...

    @abc.abstractmethod
    def create_conductor(self, **kwargs: Any) -> ConductorT:
        ...
