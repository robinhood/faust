import abc
import asyncio
import typing
from typing import Any, Awaitable, Callable, NamedTuple, Pattern, Sequence

if typing.TYPE_CHECKING:
    from .streams import Stream
    from .transport.base import Transport
else:
    class Stream: ...     # noqa
    class Transport: ...  # noqa

__all__ = [
    'K', 'V', 'Serializer',
    'Topic', 'Message', 'ConsumerCallback',
    'ServiceT', 'AppT',
]

K = str
V = Any
Serializer = Callable[[Any], Any]

class Topic(NamedTuple):
    topics: Sequence[str]
    pattern: Pattern
    type: type
    key_serializer: Serializer
    value_serializer: Serializer


class Message(NamedTuple):
    topic: str
    partition: str
    offset: int
    timestamp: float
    timestamp_type: str
    key: bytes
    value: bytes
    checksum: bytes
    serialized_key_size: int
    serialized_value_size: int

ConsumerCallback = Callable[[str, str, Message], Awaitable]

class ServiceT(metaclass=abc.ABCMeta):

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
    servers: Sequence[str]
    loop: asyncio.AbstractEventLoop

    @abc.abstractmethod
    def add_stream(self, stream: Stream) -> Stream:
        ...

    @abc.abstractmethod
    def add_task(self, task: Callable) -> Stream:
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
