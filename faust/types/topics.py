import abc
import typing
from typing import (
    Any, AsyncIterable, AsyncIterator, Awaitable,
    MutableSet, Pattern, Sequence, Type, Union,
)
from ._coroutines import StreamCoroutine
from ..utils.types.services import ServiceT
from .codecs import CodecArg
from .core import K, V
from .tuples import Message, TopicPartition

if typing.TYPE_CHECKING:
    from .app import AppT
    from .streams import StreamT
    from .transports import ConsumerT, TPorTopicSet
else:
    class AppT: ...             # noqa
    class StreamT: ...          # noqa
    class ConsumerT: ...        # noqa
    class TPorTopicSet: ...     # noqa

__all__ = ['EventT', 'TopicT', 'SourceT', 'TopicManagerT']


class EventT:

    __slots__ = ('app', 'key', 'value', 'message', '__weakref__')

    def __init__(self, app: AppT, key: K, value: V, message: Message) -> None:
        self.app: AppT = app
        self.key: K = key
        self.value: V = value
        self.message: Message = message

    async def send(self, topic: Union[str, 'TopicT'],
                   *,
                   key: Any = None) -> None:
        ...

    async def forward(self, topic: Union[str, 'TopicT'],
                      *,
                      key: Any = None) -> None:
        ...

    def attach(self, topic: Union[str, 'TopicT'], key: K, value: V,
               *,
               partition: int = None,
               key_serializer: CodecArg = None,
               value_serializer: CodecArg = None) -> None:
        ...

    def ack(self) -> None:
        ...

    async def __aenter__(self) -> 'EventT':
        ...

    async def __aexit__(self, *exc_info: Any) -> None:
        ...

    def __enter__(self) -> 'EventT':
        ...

    def __exit__(self, *exc_info: Any) -> None:
        ...


class TopicT(AsyncIterable):
    app: AppT
    topics: Sequence[str]
    pattern: Pattern
    key_type: Type
    value_type: Type

    @abc.abstractmethod
    def __init__(self, app: AppT,
                 *,
                 topics: Sequence[str] = None,
                 pattern: Union[str, Pattern] = None,
                 key_type: Type = None,
                 value_type: Type = None) -> None:
        ...

    @abc.abstractmethod
    def stream(self, coroutine: StreamCoroutine = None,
               **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    async def send(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            *,
            wait: bool = True) -> Awaitable:
        ...

    def send_soon(self, key: K, value: V,
                  partition: int = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None) -> None:
        ...

    @abc.abstractmethod
    async def maybe_declare(self) -> None:
        ...

    @abc.abstractmethod
    def derive(self,
               *,
               topics: Sequence[str] = None,
               key_type: Type = None,
               value_type: Type = None,
               prefix: str = '',
               suffix: str = '') -> 'TopicT':
        ...

    @abc.abstractmethod
    def __aiter__(self) -> AsyncIterator:
        ...


class SourceT(AsyncIterator):
    topic: TopicT

    @abc.abstractmethod
    def __init__(self, topic: TopicT) -> None:
        ...

    @abc.abstractmethod
    async def deliver(self, message: Message) -> None:
        ...

    @abc.abstractmethod
    async def put(self, value: Any) -> None:
        ...

    @abc.abstractmethod
    async def get(self) -> Any:
        ...

    @abc.abstractmethod
    def __aiter__(self) -> AsyncIterator:
        ...

    @abc.abstractmethod
    async def __anext__(self) -> EventT:
        ...


class TopicManagerT(ServiceT, MutableSet[SourceT]):

    app: AppT

    @abc.abstractmethod
    def ack_message(self, message: Message) -> None:
        ...

    @abc.abstractmethod
    def ack_offset(self, tp: TopicPartition, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def commit(self, topics: TPorTopicSet) -> bool:
        ...

    @abc.abstractmethod
    def on_partitions_assigned(self,
                               assigned: Sequence[TopicPartition]) -> None:
        ...

    @abc.abstractmethod
    def on_partitions_revoked(self,
                              revoked: Sequence[TopicPartition]) -> None:
        ...
