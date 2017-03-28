import abc
import weakref
from typing import Any, Awaitable, Generator, Optional, Type, Union
from .codecs import CodecArg
from .core import K, V
from .coroutines import StreamCoroutine
from .models import ModelT, Event
from .services import ServiceT
from .sensors import SensorT
from .streams import StreamT, TopicProcessorSequence
from .transports import ConsumerT, TransportT
from .tuples import Message, Topic

__all__ = ['TaskArg', 'AsyncSerializerT', 'AppT']

TaskArg = Generator


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

    tasks_running: int
    task_to_consumers: weakref.WeakKeyDictionary

    @classmethod
    @abc.abstractmethod
    def current_app(cls):
        ...

    @abc.abstractmethod
    def register_consumer(self, consumer: ConsumerT) -> None:
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
    def new_stream_name(self) -> str:
        ...

    @abc.abstractmethod
    async def send(
            self, topic: Union[Topic, str], key: K, value: V,
            *,
            wait: bool = True) -> Awaitable:
        ...

    @abc.abstractmethod
    async def loads_key(self, typ: Optional[Type], key: bytes) -> K:
        ...

    @abc.abstractmethod
    async def loads_value(self, typ: Type, key: K, message: Message) -> Event:
        ...

    @abc.abstractmethod
    async def dumps_key(self, topic: str, key: K) -> bytes:
        ...

    @abc.abstractmethod
    async def dumps_value(self, topic: str, value: V) -> bytes:
        ...

    @abc.abstractmethod
    async def on_event_in(
            self, consumer_id: int, offset: int, event: Event) -> None:
        ...

    @abc.abstractmethod
    async def on_event_out(
            self, consumer_id: int, offset: int, event: Event = None) -> None:
        ...

    @property
    @abc.abstractmethod
    def transport(self) -> TransportT:
        ...
