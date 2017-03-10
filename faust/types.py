from typing import Any, Awaitable, Callable, NamedTuple, Pattern, Sequence
from aiokafka.fetcher import ConsumerRecord as Message

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
