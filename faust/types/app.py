import abc
import typing
from typing import (
    Any, Awaitable, Callable, Generator,
    Iterable, Sequence, Tuple, Type, Union,
)
from ..utils.types.services import ServiceT
from ._coroutines import StreamCoroutine
from .codecs import CodecArg
from .core import K, V
from .serializers import RegistryT
from .sensors import SensorDelegateT
from .streams import Processor, StreamT, StreamManagerT, TopicProcessorSequence
from .tables import TableT
from .transports import TransportT
from .tuples import Message, PendingMessage, Topic, TopicPartition
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

    sensors: SensorDelegateT
    serializers: RegistryT

    @classmethod
    @abc.abstractmethod
    def current_app(cls) -> 'AppT':
        ...

    @abc.abstractmethod
    def task(self, fun: Callable[['AppT'], Generator] = None,
             *,
             concurrency: int = 1) -> None:
        ...

    @abc.abstractmethod
    def timer(self, interval: float) -> Callable:
        ...

    @abc.abstractmethod
    def add_task(self, task: Generator) -> Awaitable:
        ...

    @abc.abstractmethod
    def stream(self, topic: Topic,
               coroutine: StreamCoroutine = None,
               processors: TopicProcessorSequence = None,
               **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    def table(self, table_name: str,
              *,
              default: Callable[[], Any] = None,
              topic: Topic = None,
              coroutine: StreamCoroutine = None,
              processors: Sequence[Processor] = None,
              window: WindowT = None,
              **kwargs: Any) -> TableT:
        ...

    @abc.abstractmethod
    def add_source(self, stream: StreamT) -> None:
        ...

    @abc.abstractmethod
    def add_table(self, table: TableT) -> None:
        ...

    @abc.abstractmethod
    def new_stream_name(self) -> str:
        ...

    @abc.abstractmethod
    async def send(
            self, topic: Union[Topic, str], key: K, value: V,
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
            self, topic: Union[Topic, str], key: K, value: V,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None) -> None:
        ...

    @abc.abstractmethod
    def send_attached(self,
                      message: Message,
                      topic: Union[str, Topic],
                      key: K,
                      value: V,
                      partition: int = None,
                      key_serializer: CodecArg = None,
                      value_serializer: CodecArg = None) -> None:
        ...

    @abc.abstractmethod
    async def commit_attached(self, tp: TopicPartition, offset: int) -> None:
        ...

    @abc.abstractmethod
    def render_graph(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def transport(self) -> TransportT:
        ...

    @property
    @abc.abstractmethod
    def tasks_running(self) -> int:
        ...

    @property
    @abc.abstractmethod
    def website(self) -> Web:
        ...

    @property
    @abc.abstractmethod
    def streams(self) -> StreamManagerT:
        ...

    @property
    @abc.abstractmethod
    def monitor(self) -> Monitor:
        ...

    @monitor.setter
    def monitor(self, value: Monitor) -> None:
        ...
