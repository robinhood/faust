import abc
import asyncio
import typing
from typing import (
    Any, AsyncIterator, Awaitable, Callable, List, Mapping,
    MutableMapping, MutableSequence, Sequence, TypeVar, Union,
)
from .core import K
from .coroutines import CoroCallbackT, StreamCoroutine
from .models import Event, FieldDescriptorT
from .services import ServiceT
from .transports import ConsumerT
from .tuples import Message, Topic

if typing.TYPE_CHECKING:
    from .app import AppT
    from .join import JoinT
else:
    class AppT: ...   # noqa
    class JoinT: ...  # noqa

__all__ = [
    'Processor',
    'TopicProcessorSequence',
    'StreamProcessorMap',
    'StreamCoroutineMap',
    'StreamT',
    'StreamManagerT',
]

# Used for typing StreamT[Withdrawal]
_T = TypeVar('_T')

Processor = Callable[[Event], Union[Event, Awaitable[Event]]]
TopicProcessorSequence = Sequence[Processor]
StreamProcessorMap = MutableMapping[Topic, TopicProcessorSequence]
StreamCoroutineMap = MutableMapping[Topic, CoroCallbackT]


class StreamT(AsyncIterator[_T], ServiceT):

    app: AppT = None
    topics: MutableSequence[Topic] = None
    name: str = None
    outbox: asyncio.Queue = None
    join_strategy: JoinT = None

    children: List['StreamT'] = None

    @classmethod
    @abc.abstractmethod
    def from_topic(cls, topic: Topic,
                   *,
                   coroutine: StreamCoroutine = None,
                   processors: TopicProcessorSequence = None,
                   loop: asyncio.AbstractEventLoop = None,
                   **kwargs: Any) -> 'StreamT':
        ...

    def __init__(self, name: str = None,
                 topics: Sequence[Topic] = None,
                 processors: StreamProcessorMap = None,
                 coroutines: StreamCoroutineMap = None,
                 children: List['StreamT'] = None,
                 join_strategy: JoinT = None,
                 app: AppT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        # need this to initialize Service.__init__ (!)
        super().__init__(loop=loop)  # type: ignore

    @abc.abstractmethod
    def bind(self, app: AppT) -> 'StreamT':
        ...

    @abc.abstractmethod
    def _bind(self, app: AppT) -> 'StreamT':
        ...

    @abc.abstractmethod
    def info(self) -> Mapping[str, Any]:
        ...

    @abc.abstractmethod
    def clone(self, **kwargs: Any) -> 'StreamT':
        ...

    @abc.abstractmethod
    def combine(self, *nodes: 'StreamT', **kwargs: Any):
        ...

    @abc.abstractmethod
    def through(self, topic: Union[str, Topic]) -> 'StreamT':
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
    async def process(self, key: K, value: Event) -> Event:
        ...

    @abc.abstractmethod
    async def on_done(self, value: Event = None) -> None:
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
    def __next__(self) -> Event:
        ...

    @abc.abstractmethod
    async def __aiter__(self):
        ...

    @abc.abstractmethod
    async def __anext__(self) -> Any:
        ...


class StreamManagerT(ServiceT):

    consumer: ConsumerT

    @abc.abstractmethod
    def add_stream(self, stream: StreamT):
        ...

    @abc.abstractmethod
    async def update(self):
        ...
