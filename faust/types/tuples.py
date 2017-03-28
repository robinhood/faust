import faust
from typing import NamedTuple, Pattern, Sequence, Type
from .core import K

__all__ = ['Topic', 'Message', 'Request']


class Topic(NamedTuple):
    topics: Sequence[str]
    pattern: Pattern
    key_type: Type
    value_type: Type


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
    app: 'faust.types.AppT'
    key: K
    message: Message
