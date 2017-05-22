import abc
import typing
from typing import (
    Any, AsyncIterable, Awaitable, Callable,
    Iterable, MutableMapping, Pattern, Tuple, Type, Union,
)
from ..utils.times import Seconds
from ..utils.types.services import ServiceT
from ._coroutines import StreamCoroutine
from .actors import ActorFun, ActorT
from .codecs import CodecArg
from .core import K, V
from .serializers import RegistryT
from .sensors import SensorDelegateT
from .streams import Processor, StreamT
from .tables import TableT
from .transports import ConsumerT, TransportT
from .topics import TopicT, TopicManagerT
from .tuples import Message, PendingMessage, TopicPartition
from .windows import WindowT

if typing.TYPE_CHECKING:  # pragma: no cover
    from ..web.base import Web
    from ..sensors import Monitor
else:
    class Web: ...      # noqa
    class Monitor: ...  # noqa


__all__ = ['AppT']


class AppT(ServiceT):
    """Abstract type for the Faust application.

    See Also:
        :class:`faust.App`.
    """

    Stream: Type
    Table: Type
    WebSite: Type
    Serializers: Type

    id: str
    url: str
    client_id: str
    commit_interval: float
    key_serializer: CodecArg
    value_serializer: CodecArg
    num_standby_replicas: int
    replication_factor: int
    avro_registry_url: str
    store: str

    actors: MutableMapping[str, ActorT]
    tables: MutableMapping[str, TableT]
    sensors: SensorDelegateT
    serializers: RegistryT

    @abc.abstractmethod
    def topic(self, *topics: str,
              pattern: Union[str, Pattern] = None,
              key_type: Type = None,
              value_type: Type = None) -> TopicT:
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
    def table(self, table_name: str,
              *,
              default: Callable[[], Any] = None,
              coroutine: StreamCoroutine = None,
              processors: Iterable[Processor] = None,
              window: WindowT = None,
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
    def website(self) -> Web:
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
