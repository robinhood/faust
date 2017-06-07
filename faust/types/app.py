import abc
import asyncio
import typing
from typing import (
    Any, AsyncIterable, Awaitable, Callable,
    Iterable, Mapping, MutableMapping, Pattern, Tuple, Type, Union,
)
from ..utils.imports import SymbolArg
from ..utils.times import Seconds
from ..utils.types.services import ServiceT
from ._coroutines import StreamCoroutine
from .actors import ActorFun, ActorT
from .codecs import CodecArg
from .core import K, V
from .serializers import RegistryT
from .sensors import SensorDelegateT
# mypy requires this for some unknown reason, but it's not used
from .streams import T  # noqa: F401
from .streams import Processor, StreamT
from .tables import TableT, TableManagerT
from .transports import ConsumerT, TransportT
from .topics import TopicT, TopicManagerT
from .tuples import Message, PendingMessage, TopicPartition
from .windows import WindowT

if typing.TYPE_CHECKING:  # pragma: no cover
    from ..sensors import Monitor
    from .models import ModelArg
else:
    class Monitor: ...   # noqa
    class ModelArg: ...  # noqa

__all__ = ['AppT']


class AppT(ServiceT):
    """Abstract type for the Faust application.

    See Also:
        :class:`faust.App`.
    """

    Stream: Type[StreamT]
    Table: Type[TableT]
    Serializers: Type[RegistryT]

    id: str
    url: str
    client_id: str
    commit_interval: float
    table_cleanup_interval: float
    key_serializer: CodecArg
    value_serializer: CodecArg
    num_standby_replicas: int
    replication_factor: int
    default_partitions: int  # noqa: E704
    avro_registry_url: str
    store: str

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
                 commit_interval: Seconds = 1.0,
                 table_cleanup_interval: Seconds = 1.0,
                 key_serializer: CodecArg = 'json',
                 value_serializer: CodecArg = 'json',
                 num_standby_replicas: int = 0,
                 replication_factor: int = 1,
                 default_partitions: int = 8,
                 Stream: SymbolArg = '',
                 Table: SymbolArg = '',
                 Serializers: SymbolArg = '',
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
              config: Mapping[str, Any] = None) -> TopicT:
        ...

    @abc.abstractmethod
    def actor(self, topic: TopicT,
              *,
              concurrency: int = 1) -> Callable[[ActorFun], ActorT]:
        ...

    @abc.abstractmethod
    def task(self, fun: Callable[[], Awaitable]) -> Callable:
        ...

    @abc.abstractmethod
    def timer(self, interval: Seconds) -> Callable:
        ...

    @abc.abstractmethod
    def stream(self, source: AsyncIterable,
               coroutine: StreamCoroutine = None,
               **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    def table(self, name: str,
              *,
              default: Callable[[], Any] = None,
              coroutine: StreamCoroutine = None,
              processors: Iterable[Processor] = None,
              window: WindowT = None,
              partitions: int = None,
              **kwargs: Any) -> TableT:
        ...

    @abc.abstractmethod
    def add_table(self, table: TableT) -> None:
        ...

    @abc.abstractmethod
    async def send(
            self, topic: Union[TopicT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            *,
            wait: bool = True) -> Awaitable:
        ...

    @abc.abstractmethod
    async def send_many(
            self, it: Iterable[Union[PendingMessage, Tuple]]) -> None:
        ...

    @abc.abstractmethod
    def send_soon(
            self, topic: Union[TopicT, str], key: K, value: V,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None) -> None:
        ...

    @abc.abstractmethod
    def send_attached(self,
                      message: Message,
                      topic: Union[str, TopicT],
                      key: K,
                      value: V,
                      partition: int = None,
                      key_serializer: CodecArg = None,
                      value_serializer: CodecArg = None) -> None:
        ...

    @abc.abstractmethod
    async def commit_attached(self, tp: TopicPartition, offset: int) -> None:
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
    def sources(self) -> TopicManagerT:
        ...

    @property
    @abc.abstractmethod
    def monitor(self) -> Monitor:
        ...

    @monitor.setter
    def monitor(self, value: Monitor) -> None:
        ...
