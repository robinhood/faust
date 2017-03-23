"""Abstract types for static typing."""
import abc
import asyncio
from typing import (
    Any, AsyncIterable, Awaitable, Callable, Coroutine, FrozenSet, Generator,
    Generic, Iterable, List, Mapping, MutableMapping, MutableSequence,
    NamedTuple, Optional, Pattern, Sequence, Tuple, Type, TypeVar, Union,
)

__all__ = [
    'K', 'V', 'SerializerT', 'SerializerArg',
    'Topic', 'Message', 'Request', 'ConsumerCallback',
    'KeyDecodeErrorCallback', 'ValueDecodeErrorCallback',
    'ServiceT', 'AppT',
]


# Used for typing StreamT[Withdrawal]
_T = TypeVar('_T')

#: Shorthand for the type of a key
K = Optional[Union[bytes, 'MessageTypeT']]


class SerializerT(metaclass=abc.ABCMeta):
    """Abstract type for Serializer.

    See Also:
        :class:`faust.utils.serialization.Serializer`.
    """

    @abc.abstractmethod
    def dumps(self, obj: Any) -> bytes:
        ...

    @abc.abstractmethod
    def loads(self, s: bytes) -> Any:
        ...

    @abc.abstractmethod
    def clone(self, *children: 'SerializerT') -> 'SerializerT':
        ...

    @abc.abstractmethod
    def __or__(self, other: Any) -> Any:
        ...


# `serializer` argument can be str or serializer instance.
SerializerArg = Union[SerializerT, str]


class Topic(NamedTuple):
    topics: Sequence[str]
    pattern: Pattern
    type: Type
    key_serializer: SerializerArg


class Message(NamedTuple):
    topic: str
    partition: int
    offset: int
    timestamp: float
    timestamp_type: str
    key: bytes
    value: bytes
    checksum: bytes
    serialized_key_size: int
    serialized_value_size: int


class Request(NamedTuple):
    app: 'AppT'
    key: K
    message: Message


#: Callback called by :class:`faust.transport.base.Consumer` whenever
#: a message is received.
ConsumerCallback = Callable[[Topic, 'K', 'MessageTypeT'], Awaitable]

#: Callback called by :class:`faust.transport.base.Consumer` whenever
#: a message key cannot be decoded/deserialized.
KeyDecodeErrorCallback = Callable[[Exception, Message], Awaitable]

#: Callback called by :class:`faust.transport.base.Consumer` whenever
#: a message value cannot be decoded/deserialized.
ValueDecodeErrorCallback = Callable[[Exception, Message], Awaitable]


class ServiceT(metaclass=abc.ABCMeta):
    """Abstract type for an asynchronous service that can be started/stopped.

    See Also:
        :class:`faust.utils.service.Service`.
    """

    shutdown_timeout: float
    loop: asyncio.AbstractEventLoop = None

    @abc.abstractmethod
    async def __aenter__(self) -> 'ServiceT':
        ...

    @abc.abstractmethod
    async def __aexit__(*exc_info) -> None:
        ...

    @abc.abstractmethod
    def on_init(self) -> None:
        ...

    @abc.abstractmethod
    async def on_start(self) -> None:
        ...

    @abc.abstractmethod
    async def on_stop(self) -> None:
        ...

    @abc.abstractmethod
    async def on_shutdown(self) -> None:
        ...

    @abc.abstractmethod
    async def start(self) -> None:
        ...

    @abc.abstractmethod
    async def maybe_start(self) -> None:
        ...

    @abc.abstractmethod
    async def stop(self) -> None:
        ...

    @abc.abstractmethod
    def add_poller(self, callback: Callable[[], Awaitable[None]]) -> None:
        ...

    @property
    @abc.abstractmethod
    def state(self) -> str:
        ...


class MessageTypeOptions:
    serializer: SerializerArg
    namespace: str

    # Index: Flattened view of __annotations__ in MRO order.
    fields: Mapping[str, Type]

    # Index: Set of required field names, for fast argument checking.
    fieldset: FrozenSet[str]

    # Index: Set of optional field names, for fast argument checking.
    optionalset: FrozenSet[str]

    defaults: Mapping[str, Any]  # noqa: E704 (flake8 bug)


class MessageTypeT:
    # uses __init_subclass__ so cannot use ABCMeta

    req: Request

    _options: MessageTypeOptions

    @classmethod
    def as_schema(cls) -> Mapping:
        ...

    @classmethod
    def loads(
            cls, s: bytes,
            *,
            default_serializer: SerializerArg = None,
            **kwargs) -> 'MessageTypeT':
        ...

    def from_message(
            cls, key: 'K', message: Message, app: 'AppT',
            *,
            default_serializer: SerializerArg = None) -> 'MessageTypeT':
        ...

    def dumps(self) -> bytes:
        ...

    def derive(self, *events: 'MessageTypeT', **fields) -> 'MessageTypeT':
        ...

    async def forward(self, topic: Union[str, Topic]) -> None:
        ...


#: Shorthand for the type of a value
V = MessageTypeT
Processor = Callable[[V], V]
TopicProcessorSequence = Sequence[Processor]
StreamProcessorMap = MutableMapping[Topic, TopicProcessorSequence]
StreamCoroutineMap = MutableMapping[Topic, 'CoroCallbackT']


class FieldDescriptorT:
    field: str
    type: Type
    event: Type
    required: bool = True
    default: Any = None  # noqa: E704


class InputStreamT(Iterable, AsyncIterable):
    queue: asyncio.Queue

    @abc.abstractmethod
    async def put(self, value: V) -> None:
        ...

    @abc.abstractmethod
    async def next(self) -> Any:
        ...

    @abc.abstractmethod
    async def join(self, timeout: float = None):
        ...


StreamCoroutineCallback = Callable[[V], Awaitable[None]]


class CoroCallbackT:

    def __init__(self, inbox: InputStreamT,
                 *,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    async def send(self, value: V, callback: StreamCoroutineCallback) -> None:
        ...

    async def join(self) -> None:
        ...

    async def drain(self, callback: StreamCoroutineCallback) -> None:
        ...


StreamCoroutine = Union[
    Callable[[InputStreamT], Coroutine[V, None, None]],
    Callable[[InputStreamT], AsyncIterable[V]],
    Callable[[InputStreamT], Generator[V, None, None]],
]


class EventRefT(metaclass=abc.ABCMeta):
    offset: int


class ConsumerT(ServiceT):

    id: int
    topic: Topic
    transport: 'TransportT'
    commit_interval: float

    @abc.abstractmethod
    async def _commit(self, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def register_timers(self) -> None:
        ...

    @abc.abstractmethod
    async def on_message(self, message: Message) -> None:
        ...

    @abc.abstractmethod
    def to_KV(self, message: Message) -> Tuple[K, V]:
        ...

    @abc.abstractmethod
    def track_event(self, event: V, offset: int) -> None:
        ...

    @abc.abstractmethod
    def on_event_ready(self, ref: EventRefT) -> None:
        ...


class ProducerT(ServiceT):
    transport: 'TransportT'

    @abc.abstractmethod
    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        ...

    @abc.abstractmethod
    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        ...


class TransportT(metaclass=abc.ABCMeta):
    Consumer: Type
    Producer: Type

    app: 'AppT'
    url: str
    loop: asyncio.AbstractEventLoop

    def create_consumer(self, topic: Topic, callback: ConsumerCallback,
                        **kwargs) -> ConsumerT:
        ...

    def create_producer(self, **kwargs) -> ProducerT:
        ...


TaskArg = Union[Generator, Awaitable]


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
    key_serializer: SerializerArg
    value_serializer: SerializerArg
    num_standby_replicas: int
    replication_factor: int

    @abc.abstractmethod
    def add_task(self, task: TaskArg) -> asyncio.Future:
        ...

    @abc.abstractmethod
    def stream(self, topic: Topic,
               coroutine: StreamCoroutine = None,
               processors: TopicProcessorSequence = None,
               **kwargs) -> 'StreamT':
        ...

    @abc.abstractmethod
    def add_source(self, stream: 'StreamT') -> None:
        ...

    @abc.abstractmethod
    def new_stream_name(self) -> str:
        ...

    @abc.abstractmethod
    async def send(
            self, topic: Union[Topic, str], key: K, value: V,
            *,
            wait: bool = True,
            key_serializer: SerializerArg = None) -> Awaitable:
        ...

    @property
    @abc.abstractmethod
    def transport(self) -> TransportT:
        ...


class StreamT(AsyncIterable[_T], ServiceT):

    app: AppT = None
    topics: MutableSequence[Topic] = None
    name: str = None
    outbox: asyncio.Queue = None
    join_strategy: 'JoinT' = None

    children: List['StreamT'] = None

    @classmethod
    @abc.abstractmethod
    def from_topic(cls, topic: Topic,
                   *,
                   coroutine: StreamCoroutine = None,
                   processors: TopicProcessorSequence = None,
                   loop: asyncio.AbstractEventLoop = None,
                   **kwargs) -> 'StreamT':
        ...

    def __init__(self, name: str = None,
                 topics: Sequence[Topic] = None,
                 processors: StreamProcessorMap = None,
                 coroutines: StreamCoroutineMap = None,
                 children: List['StreamT'] = None,
                 join_strategy: 'JoinT' = None,
                 app: AppT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        # need this to initialize Service.__init__ (!)
        super().__init__(loop=loop)  # type: ignore

    @abc.abstractmethod
    def bind(self, app: AppT) -> 'StreamT':
        ...

    @abc.abstractmethod
    def info(self) -> Mapping[str, Any]:
        ...

    @abc.abstractmethod
    def clone(self, **kwargs) -> 'StreamT':
        ...

    @abc.abstractmethod
    def combine(self, *nodes: 'StreamT', **kwargs):
        ...

    @abc.abstractmethod
    async def through(self, topic: Union[str, Topic]) -> AsyncIterable[V]:
        ...

    @abc.abstractmethod
    def join(self, *fields: FieldDescriptorT) -> 'StreamT':
        ...

    @abc.abstractmethod
    def left_join(self, *fields: FieldDescriptorT) -> 'StreamT':
        ...

    @abc.abstractmethod
    def inner_join(self, *fields: FieldDescriptorT) -> 'StreamT':
        ...

    @abc.abstractmethod
    def outer_join(self, *fields: FieldDescriptorT) -> 'StreamT':
        ...

    @abc.abstractmethod
    async def on_message(self, topic: Topic, key: K, value: V) -> None:
        ...

    @abc.abstractmethod
    async def process(self, key: K, value: V) -> V:
        ...

    @abc.abstractmethod
    async def on_done(self, value: V = None) -> None:
        ...

    @abc.abstractmethod
    async def subscribe(self, topic: Topic,
                        *,
                        processors: TopicProcessorSequence = None,
                        coroutine: StreamCoroutine = None) -> None:
        ...

    @abc.abstractmethod
    async def unsubscribe(self, topic: Topic) -> None:
        ...

    @abc.abstractmethod
    async def on_key_decode_error(
            self, exc: Exception, message: Message) -> None:
        ...

    @abc.abstractmethod
    async def on_value_decode_error(
            self, exc: Exception, message: Message) -> None:
        ...

    @abc.abstractmethod
    def __and__(self, other: 'StreamT') -> 'StreamT':
        ...

    @abc.abstractmethod
    def __copy__(self) -> 'StreamT':
        ...

    @abc.abstractmethod
    def __iter__(self) -> Any:
        ...

    @abc.abstractmethod
    def __next__(self) -> V:
        ...

    @abc.abstractmethod
    async def __aiter__(self) -> 'StreamT':
        ...

    @abc.abstractmethod
    async def __anext__(self) -> Any:
        ...


class JoinT(metaclass=abc.ABCMeta):
    fields: MutableMapping[Type, FieldDescriptorT]
    stream: StreamT

    @abc.abstractmethod
    async def process(_self, event: V) -> Optional[V]:
        ...
