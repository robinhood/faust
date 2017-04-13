import abc
from typing import Any, Awaitable, Optional, Type, Union
from .codecs import CodecArg
from .core import K, V, TaskArg
from .coroutines import StreamCoroutine
from .models import ModelT, Event
from .services import ServiceT
from .sensors import SensorT
from .streams import StreamT, StreamManagerT, TopicProcessorSequence
from .tables import TableT
from .transports import TransportT
from .tuples import Message, Topic

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

    streams: StreamManagerT

    @classmethod
    @abc.abstractmethod
    def current_app(cls) -> 'AppT':
        ...

    @abc.abstractmethod
    def add_task(self, task: TaskArg) -> Awaitable:
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
            wait: bool = True) -> Awaitable:
        ...

    @abc.abstractmethod
    def send_soon(
            self, topic: Union[Topic, str], key: K, value: V) -> None:
        ...

    @abc.abstractmethod
    async def loads_key(self, typ: Optional[Type], key: bytes) -> K:
        ...

    @abc.abstractmethod
    async def loads_value(self, typ: Type, key: K, message: Message) -> Event:
        ...

    @abc.abstractmethod
    async def dumps_key(self, topic: str, key: K) -> Optional[bytes]:
        ...

    @abc.abstractmethod
    async def dumps_value(self, topic: str, value: V) -> Optional[bytes]:
        ...

    @abc.abstractmethod
    async def on_message_in(
            self,
            consumer_id: int,
            offset: int,
            message: Message) -> None:
        ...

    @abc.abstractmethod
    async def on_message_out(
            self,
            consumer_id: int,
            offset: int,
            message: Message = None) -> None:
        ...

    @property
    @abc.abstractmethod
    def transport(self) -> TransportT:
        ...

    @property
    @abc.abstractmethod
    def tasks_running(self) -> int:
        ...
