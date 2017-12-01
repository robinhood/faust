import asyncio
import typing
from typing import Any, Awaitable, Callable, Dict, NamedTuple, Sequence, Union
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
    'FutureMessage', 'MessageSentCallback', 'TP',
    'PendingMessage', 'RecordMetadata', 'Message',
]

MessageSentCallback = Callable[['FutureMessage'], Union[None, Awaitable[None]]]


class TP(NamedTuple):
    topic: str
    partition: int


class RecordMetadata(NamedTuple):
    topic: str
    partition: int
    topic_partition: TP
    offset: int


class PendingMessage(NamedTuple):
    channel: ChannelT
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


def _get_len(s: bytes) -> int:
    return len(s) if s is not None and isinstance(s, bytes) else 0


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
        'time_in',
        'time_out',
        'time_total',
        'tp',
        'stream_meta',
        '__weakref__',
    )

    def __init__(self, topic: str, partition: int, offset: int,
                 timestamp: float, timestamp_type: str,
                 key: bytes, value: bytes, checksum: bytes,
                 serialized_key_size: int = None,
                 serialized_value_size: int = None,
                 tp: TP = None,
                 time_in: float = None,
                 time_out: float = None,
                 time_total: float = None) -> None:
        self.topic: str = topic
        self.partition: int = partition
        self.offset: int = offset
        self.timestamp: float = timestamp
        self.timestamp_type: str = timestamp_type
        self.key: bytes = key
        self.value: bytes = value
        self.checksum: bytes = checksum
        self.serialized_key_size: int = (
            _get_len(key)
            if serialized_key_size is None else serialized_key_size)
        self.serialized_value_size: int = (
            _get_len(value)
            if serialized_value_size is None else serialized_value_size)
        self.acked: bool = False
        self.refcount: int = 0
        self.tp = tp
        if typing.TYPE_CHECKING:
            # mypy supports this, but Python doesn't.
            self.channels: WeakSet[ChannelT] = WeakSet()
        else:
            self.channels = WeakSet()
        #: Monotonic timestamp of when the consumer received this message.
        self.time_in: float = time_in
        #: Monotonic timestamp of when the consumer acknowledged this message.
        self.time_out: float = time_out
        #: Total processing time (in seconds), or None if the event is
        #: still processing.
        self.time_total: float = time_total
        self.stream_meta: Dict[int, Any] = {}

    def incref(self, channel: ChannelT = None, n: int = 1) -> None:
        self.channels.add(channel)
        self.refcount += n

    def incref_bulk(self, channels: Sequence[ChannelT]) -> None:
        self.channels.update(channels)
        self.refcount += len(channels)

    def decref(self, n: int = 1) -> None:
        self.refcount = max(self.refcount - 1, 0)

    @classmethod
    def from_message(cls, message: Any, tp: TP) -> 'Message':
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


# XXX See top of module! We redefine this with final FutureMessage
# for Sphinx as it cannot read non-final types.
MessageSentCallback = Callable[  # type: ignore
    [FutureMessage], Union[None, Awaitable[None]]]
