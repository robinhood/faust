import abc
import asyncio
import typing
from typing import (
    Any, AsyncIterable, Awaitable, Callable,
    Iterable, Mapping, MutableMapping, Pattern, Type, Union,
)
from ._coroutines import StreamCoroutine
from .actors import ActorFun, ActorT, SinkT
from .assignor import PartitionAssignorT
from .codecs import CodecArg
from .core import K, V
from .sensors import SensorDelegateT
from .serializers import RegistryT
# mypy requires this for some unknown reason, but it's not used
from .streams import T  # noqa: F401
from .streams import StreamT
from .tables import CollectionT, SetT, TableManagerT, TableT
from .topics import ChannelT, TopicManagerT, TopicT
from .transports import ConsumerT, ProducerT, TransportT
from .tuples import (
    Message, MessageSentCallback, RecordMetadata, TopicPartition,
)
from .windows import WindowT
from ..utils.imports import SymbolArg
from ..utils.times import Seconds
from ..utils.types.collections import NodeT
from ..utils.types.services import ServiceT

if typing.TYPE_CHECKING:  # pragma: no cover
    from ..sensors import Monitor
    from .models import ModelArg
else:
    class Monitor: ...        # noqa
    class ModelArg: ...       # noqa

__all__ = ['AppT']

__flake8_RecordMetadata_is_used: RecordMetadata  # XXX flake8 bug


class AppT(ServiceT):
    """Abstract type for the Faust application.

    See Also:
        :class:`faust.App`.
    """

    Stream: Type[StreamT]
    TableType: Type[TableT]
    TableManager: Type[TableManagerT]
    SetType: Type[SetT]
    Serializers: Type[RegistryT]

    id: str
    url: str
    client_id: str
    client_only: bool
    commit_interval: float
    table_cleanup_interval: float
    key_serializer: CodecArg
    value_serializer: CodecArg
    num_standby_replicas: int
    replication_factor: int
    default_partitions: int  # noqa: E704
    reply_to: str
    create_reply_topic: float
    reply_expires: float
    avro_registry_url: str
    store: str
    assignor: PartitionAssignorT

    actors: MutableMapping[str, ActorT]
    sensors: SensorDelegateT
    serializers: RegistryT

    @abc.abstractmethod
    def __init__(self, id: str,
                 *,
                 url: str = 'aiokafka://localhost:9092',
                 store: str = 'memory://',
                 avro_registry_url: str = None,
                 client_id: str = '',
                 commit_interval: Seconds = 9999.0,
                 table_cleanup_interval: Seconds = 9999.0,
                 key_serializer: CodecArg = 'json',
                 value_serializer: CodecArg = 'json',
                 num_standby_replicas: int = 0,
                 replication_factor: int = 1,
                 default_partitions: int = 8,
                 reply_to: str = None,
                 reply_expires: Seconds = 9999.0,
                 Stream: SymbolArg[Type[StreamT]] = '',
                 Table: SymbolArg[Type[TableT]] = '',
                 TableManager: SymbolArg[Type[TableManagerT]] = '',
                 Set: SymbolArg[Type[SetT]] = '',
                 Serializers: SymbolArg[Type[RegistryT]] = '',
                 monitor: Monitor = None,
                 on_startup_finished: Callable = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.on_startup_finished: Callable = None

    @abc.abstractmethod
    def topic(self, *topics: str,
              pattern: Union[str, Pattern] = None,
              key_type: ModelArg = None,
              value_type: ModelArg = None,
              partitions: int = None,
              retention: Seconds = None,
              compacting: bool = None,
              deleting: bool = None,
              replicas: int = None,
              config: Mapping[str, Any] = None) -> TopicT:
        ...

    @abc.abstractmethod
    def channel(self, *,
                key_type: ModelArg = None,
                value_type: ModelArg = None,
                loop: asyncio.AbstractEventLoop = None) -> ChannelT:
        ...

    @abc.abstractmethod
    def actor(self,
              channel: Union[str, ChannelT] = None,
              *,
              name: str = None,
              concurrency: int = 1,
              sink: Iterable[SinkT] = None) -> Callable[[ActorFun], ActorT]:
        ...

    @abc.abstractmethod
    def task(self, fun: Callable[[], Awaitable]) -> Callable:
        ...

    @abc.abstractmethod
    def timer(self, interval: Seconds) -> Callable:
        ...

    @abc.abstractmethod
    def stream(self, channel: AsyncIterable,
               coroutine: StreamCoroutine = None,
               beacon: NodeT = None,
               **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    def Table(self, name: str,
              *,
              default: Callable[[], Any] = None,
              window: WindowT = None,
              partitions: int = None,
              **kwargs: Any) -> TableT:
        ...

    @abc.abstractmethod
    def Set(self, name: str,
            *,
            window: WindowT = None,
            partitions: int = None,
            **kwargs: Any) -> SetT:
        ...

    @abc.abstractmethod
    def add_collection(self, table: CollectionT) -> None:
        ...

    @abc.abstractmethod
    async def start_client(self) -> None:
        ...

    @abc.abstractmethod
    async def maybe_start_client(self) -> None:
        ...

    @abc.abstractmethod
    async def maybe_attach(
            self,
            channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None,
            force: bool = False) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    async def send(
            self, channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    def send_soon(
            self, channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    def send_attached(
            self,
            message: Message,
            channel: Union[str, ChannelT],
            key: K,
            value: V,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    async def commit_attached(self, tp: TopicPartition, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def maybe_start_producer(self) -> ProducerT:
        ...

    @property
    @abc.abstractmethod
    def transport(self) -> TransportT:
        ...

    @property
    @abc.abstractmethod
    def consumer(self) -> ConsumerT:
        ...

    @property
    @abc.abstractmethod
    def tables(self) -> TableManagerT:
        ...

    @property
    @abc.abstractmethod
    def channels(self) -> TopicManagerT:
        ...

    @property
    @abc.abstractmethod
    def monitor(self) -> Monitor:
        ...

    @monitor.setter
    def monitor(self, value: Monitor) -> None:
        ...
