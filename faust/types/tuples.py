import asyncio
import typing
from typing import Any, Awaitable, Callable, NamedTuple, Sequence, Union
from weakref import WeakSet
from .codecs import CodecArg
from .core import K, V

if typing.TYPE_CHECKING:
    from .app import AppT
    from .channels import ChannelT
else:
    class AppT: ...      # noqa
    class ChannelT: ...   # noqa

__all__ = [
    'FutureMessage', 'MessageSentCallback', 'TopicPartition',
    'PendingMessage', 'RecordMetadata', 'Message',
]

MessageSentCallback = Callable[['FutureMessage'], Union[None, Awaitable[None]]]


class TopicPartition(NamedTuple):
    topic: str
    partition: int


class RecordMetadata(NamedTuple):
    topic: str
    partition: int
    topic_partition: TopicPartition
    offset: int


class PendingMessage(NamedTuple):
    topic: Union[str, ChannelT]
    key: K
    value: V
    partition: int
    key_serializer: CodecArg
    value_serializer: CodecArg
    callback: MessageSentCallback


class FutureMessage(asyncio.Future, Awaitable[RecordMetadata]):
    message: PendingMessage

    def __init__(self, message: PendingMessage) -> None:
        self.message = message
        super().__init__()

    def set_result(self, result: RecordMetadata) -> None:
        super().set_result(result)


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
        'refcount',
        'channels',
        'tp',
        '__weakref__',
    )

    def __init__(self, topic: str, partition: int, offset: int,
                 timestamp: float, timestamp_type: str,
                 key: bytes, value: bytes, checksum: bytes,
                 serialized_key_size: int = None,
                 serialized_value_size: int = None,
                 tp: TopicPartition = None) -> None:
        self.topic: str = topic
        self.partition: int = partition
        self.offset: int = offset
        self.timestamp: float = timestamp
        self.timestamp_type: str = timestamp_type
        self.key: bytes = key
        self.value: bytes = value
        self.checksum: bytes = checksum
        self.serialized_key_size: int = serialized_key_size or len(key)
        self.serialized_value_size: int = serialized_value_size or len(value)
        self.acked: bool = False
        self.refcount: int = 0
        self.tp = tp
        if typing.TYPE_CHECKING:
            # mypy supports this, but Python doesn't.
            self.channels: WeakSet[ChannelT] = WeakSet()
        else:
            self.channels = WeakSet()

    def incref(self, channel: ChannelT = None, n: int = 1) -> None:
        self.channels.add(channel)
        self.refcount += n

    def incref_bulk(self, channels: Sequence[ChannelT]) -> None:
        self.channels.update(channels)
        self.refcount += len(channels)

    def decref(self, n: int = 1) -> None:
        self.refcount -= n
        assert self.refcount >= 0

    @classmethod
    def from_message(cls, message: Any, tp: TopicPartition) -> 'Message':
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
            tp,
        )

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self.tp} offset={self.offset}>'
