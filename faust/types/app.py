import abc
import typing
from typing import (
    Any, Awaitable, Callable, Generator,
    Optional, Sequence, Type, Union,
)
from ..utils.types.services import ServiceT
from ._coroutines import StreamCoroutine
from .codecs import CodecArg
from .core import K, V
from .models import ModelT, Event
from .sensors import SensorT
from .streams import Processor, StreamT, StreamManagerT, TopicProcessorSequence
from .tables import TableT
from .transports import TransportT
from .tuples import Message, Topic, TopicPartition
from .windows import WindowT

if typing.TYPE_CHECKING:  # pragma: no cover
    from .web.base import Web
else:
    class Web: ...  # noqa


__all__ = ['AsyncSerializerT', 'AppT']


class AsyncSerializerT:
    app: 'AppT'

    async def loads(self, s: bytes) -> Any:
        ...

    async def dumps_key(self, topic: str, s: ModelT) -> bytes:
        ...

    async def dumps_value(self, topic: str, s: ModelT) -> bytes:
        ...


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

    @classmethod
    @abc.abstractmethod
    def current_app(cls) -> 'AppT':
        ...

    @abc.abstractmethod
    def add_task(self, task: Generator) -> Awaitable:
        ...

    @abc.abstractmethod
    def add_sensor(self, sensor: SensorT) -> None:
        ...

    @abc.abstractmethod
    def remove_sensor(self, sensor: SensorT) -> None:
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
            *,
            wait: bool = True,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None) -> Awaitable:
        ...

    @abc.abstractmethod
    def send_soon(
            self, topic: Union[Topic, str], key: K, value: V,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None) -> None:
        ...

    @abc.abstractmethod
    def send_attached(self,
                      message: Message,
                      topic: Union[str, Topic],
                      key: K,
                      value: V,
                      *,
                      key_serializer: CodecArg = None,
                      value_serializer: CodecArg = None) -> None:
        ...

    @abc.abstractmethod
    def commit_attached(self, tp: TopicPartition, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def loads_key(self, typ: Optional[Type], key: bytes) -> K:
        ...

    @abc.abstractmethod
    async def loads_value(self, typ: Type, key: K, message: Message) -> Event:
        ...

    @abc.abstractmethod
    async def dumps_key(self, topic: str, key: K,
                        serializer: CodecArg = None) -> Optional[bytes]:
        ...

    @abc.abstractmethod
    async def dumps_value(self, topic: str, value: V,
                          serializer: CodecArg = None) -> Optional[bytes]:
        ...

    @abc.abstractmethod
    async def on_message_in(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message) -> None:
        ...

    @abc.abstractmethod
    async def on_stream_event_in(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: Event) -> None:
        ...

    @abc.abstractmethod
    async def on_stream_event_out(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: Event) -> None:
        ...

    @abc.abstractmethod
    async def on_message_out(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message = None) -> None:
        ...

    @abc.abstractmethod
    def on_table_get(self, table: TableT, key: Any) -> None:
        ...

    @abc.abstractmethod
    def on_table_set(self, table: TableT, key: Any, value: Any) -> None:
        ...

    @abc.abstractmethod
    def on_table_del(self, table: TableT, key: Any) -> None:
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
