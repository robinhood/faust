"""Abstract types for static typing."""
import abc
import asyncio
import typing
from typing import (
    Any, Awaitable, Callable, Generator,
    NamedTuple, Pattern, Sequence, Type, Union,
)

if typing.TYPE_CHECKING:
    from .streams import Stream
    from .transport.base import Transport
else:
    class Stream: ...     # noqa
    class Transport: ...  # noqa

__all__ = [
    'K', 'V', 'SerializerT', 'SerializerArg',
    'Topic', 'Message', 'Request', 'ConsumerCallback',
    'KeyDecodeErrorCallback', 'ValueDecodeErrorCallback',
    'ServiceT', 'AppT',
]

#: Shorthand for the type of a key (Any for now).
K = Any

#: Shorthand for the type of a value (Any for now).
V = Any


class SerializerT(metaclass=abc.ABCMeta):
    """Abstract type for Serializer.

    See Also:
        :class:`faust.utils.serialization.Serializer`.
    """

    @abc.abstractmethod
    def dumps(self, obj: Any) -> Any:
        ...

    @abc.abstractmethod
    def loads(self, s: Any) -> Any:
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
    key: K
    message: Message


#: Callback called by :class:`faust.transport.base.Consumer` whenever
#: a message is received.
ConsumerCallback = Callable[[Topic, K, V], Awaitable]

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
    loop: asyncio.AbstractEventLoop

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
    def add_poller(self, callback: Callable) -> None:
        ...

    @property
    @abc.abstractmethod
    def state(self) -> str:
        ...


class AppT(ServiceT):
    """Abstract type for the Faust application.

    See Also:
        :class:`faust.App`.
    """

    id: str
    url: str
    client_id: str
    commit_interval: float
    key_serializer: SerializerArg
    value_serializer: SerializerArg
    num_standby_replicas: int
    replication_factor: int
    loop: asyncio.AbstractEventLoop

    @abc.abstractmethod
    def add_stream(self, stream: Stream) -> Stream:
        ...

    @abc.abstractmethod
    def add_task(self, task: Union[Generator, Awaitable]) -> asyncio.Future:
        ...

    @abc.abstractmethod
    def add_source(self, stream: Stream) -> None:
        ...

    @abc.abstractmethod
    def new_stream_name(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def transport(self) -> Transport:
        ...
