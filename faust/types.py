import abc
from typing import Any, Awaitable, Callable, NamedTuple, Pattern, Sequence
from aiokafka.fetcher import ConsumerRecord as Message

if 0:
    from .streams import Stream
    from .task import Task

__all__ = ['K', 'V', 'Serializer']
K = str
V = Any
Serializer = Callable[[Any], Any]
ConsumerCallback = Callable[[str, str, Message], Awaitable]


class Topic(NamedTuple):
    topics: Sequence[str]
    pattern: Pattern
    type: type
    key_serializer: Serializer
    value_serializer: Serializer


class AppT(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def add_stream(self, stream: Stream) -> Stream:
        ...

    @abc.abstractmethod
    def add_task(self, task: Task) -> Stream:
        ...

    @abc.abstractmethod
    def add_source(self, stream: Stream) -> None:
        ...

    @abc.abstractmethod
    def new_stream_name(self) -> str:
        ...
