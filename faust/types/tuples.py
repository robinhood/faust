import typing
from typing import Any, NamedTuple, Pattern, Sequence, Type, Union
from .codecs import CodecArg
from .core import K, V

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa

__all__ = ['Topic', 'TopicPartition', 'Message', 'Request']


class Topic(NamedTuple):
    topics: Sequence[str]
    pattern: Pattern
    key_type: Type
    value_type: Type


class TopicPartition(NamedTuple):
    topic: str
    partition: int


class PendingMessage(NamedTuple):
    topic: Union[str, Topic]
    key: K
    value: V
    key_serializer: CodecArg
    value_serializer: CodecArg


class Message:

    __slots__ = (
        'topic',
        'partition',
        'offset',
        'timestamp',
        'timestamp_type',
        'key',
        'value',
        'checksum',
        'serialized_key_size',
        'serialized_value_size',
        'acked',
        '__weakref__',
    )

    def __init__(self, topic: str, partition: int, offset: int,
                 timestamp: float, timestamp_type: str,
                 key: bytes, value: bytes, checksum: bytes,
                 serialized_key_size: int = None,
                 serialized_value_size: int = None) -> None:
        self.topic: str = topic
        self.partition: int = partition
        self.offset: int = offset
        self.timestamp: float = timestamp
        self.timestamp_type: str = timestamp_type
        self.key: bytes = key
        self.value: bytes = value
        self.checksum: bytes = checksum
        self.serialized_key_size = serialized_key_size or len(key)
        self.serialized_value_size = serialized_value_size or len(value)
        self.acked = False

    @classmethod
    def from_message(cls, message: Any) -> 'Message':
        return cls(
            message.topic,
            message.partition,
            message.offset,
            message.timestamp,
            message.timestamp_type,
            message.key,
            message.value,
            message.checksum,
            message.serialized_key_size,
            message.serialized_value_size,
        )

    def __repr__(self) -> str:
        return '<{name}: {self.topic} {self.partition} {self.offset}'.format(
            name=type(self).__name__,
            self=self,
        )


class Request(NamedTuple):
    app: AppT
    key: K
    message: Message
