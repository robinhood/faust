import asyncio
from time import time
import typing
from collections import defaultdict
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    MutableMapping,
    NamedTuple,
    Optional,
    Set,
    Union,
    cast,
)

from .codecs import CodecArg
from .core import HeadersArg, K, OpenHeadersArg, V

if typing.TYPE_CHECKING:
    from .channels import ChannelT as _ChannelT
    from .transports import ConsumerT as _ConsumerT
else:
    class _ChannelT: ...   # noqa
    class _ConsumerT: ...  # noqa

__all__ = [
    'ConsumerMessage',
    'FutureMessage',
    'Message',
    'MessageSentCallback',
    'PendingMessage',
    'RecordMetadata',
    'TP',
    'tp_set_to_map',
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
    timestamp: Optional[float] = None
    timestamp_type: Optional[int] = None


class PendingMessage(NamedTuple):
    channel: _ChannelT
    key: K
    value: V
    partition: Optional[int]
    timestamp: Optional[float]
    headers: Optional[OpenHeadersArg]
    key_serializer: CodecArg
    value_serializer: CodecArg
    callback: Optional[MessageSentCallback]
    topic: Optional[str] = None
    offset: Optional[int] = None


def _PendingMessage_to_Message(p: PendingMessage) -> 'Message':
    # CPython3.6.0 does not support methods on NamedTuple [ask]

    # In-memory channel.send uses this to convert
    # PendingMessage to Message.
    topic = cast(str, p.topic)
    partition = cast(int, p.partition) or 0
    tp = TP(topic, partition)
    timestamp = cast(float, p.timestamp) or time()
    timestamp_type = 1 if p.timestamp else 0
    return Message(
        topic,
        partition,
        -1,
        timestamp=timestamp,
        timestamp_type=timestamp_type,
        headers=p.headers,
        key=p.key,
        value=p.value,
        checksum=None,
        tp=tp,
    )


class FutureMessage(asyncio.Future, Awaitable[RecordMetadata]):
    message: PendingMessage

    def __init__(self, message: PendingMessage) -> None:
        self.message = message
        super().__init__()

    def set_result(self, result: RecordMetadata) -> None:
        super().set_result(result)


def _get_len(s: Optional[bytes]) -> int:
    return len(s) if s is not None and isinstance(s, bytes) else 0


class Message:

    __slots__ = (
        'topic',
        'partition',
        'offset',
        'timestamp',
        'timestamp_type',
        'headers',
        'key',
        'value',
        'checksum',
        'serialized_key_size',
        'serialized_value_size',
        'acked',
        'refcount',
        'time_in',
        'time_out',
        'time_total',
        'tp',
        'tracked',
        'stream_meta',
        'span',
        '__weakref__',
    )

    use_tracking: bool = False

    def __init__(self,
                 topic: str,
                 partition: int,
                 offset: int,
                 timestamp: float,
                 timestamp_type: int,
                 headers: Optional[HeadersArg],
                 key: Optional[bytes],
                 value: Optional[bytes],
                 checksum: Optional[bytes],
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
        self.timestamp_type: int = timestamp_type
        self.headers: Optional[HeadersArg] = headers
        self.key: Optional[bytes] = key
        self.value: Optional[bytes] = value
        self.checksum: Optional[bytes] = checksum
        self.serialized_key_size: int = (
            _get_len(key)
            if serialized_key_size is None else serialized_key_size)
        self.serialized_value_size: int = (
            _get_len(value)
            if serialized_value_size is None else serialized_value_size)
        self.acked: bool = False
        self.refcount: int = 0
        self.tp = tp if tp is not None else TP(topic, partition)
        self.tracked: bool = not self.use_tracking

        #: Monotonic timestamp of when the consumer received this message.
        self.time_in: Optional[float] = time_in
        #: Monotonic timestamp of when the consumer acknowledged this message.
        self.time_out: Optional[float] = time_out
        #: Total processing time (in seconds), or None if the event is
        #: still processing.
        self.time_total: Optional[float] = time_total
        #: Monitor stores timing information for every stream
        #: processing this message here.  It's stored as::
        #:
        #:    messsage.stream_meta[id(stream)] = {
        #:        'time_in': float,
        #:        'time_out': float,
        #:        'time_total': float,
        #:   }
        self.stream_meta: Dict[int, Any] = {}

    def ack(self, consumer: _ConsumerT, n: int = 1) -> bool:
        if not self.acked:
            # if no more references, mark offset as safe-to-commit in
            # Consumer.
            if not self.decref(n):
                return self.on_final_ack(consumer)
        return False

    def on_final_ack(self, consumer: _ConsumerT) -> bool:
        self.acked = True
        return True

    def incref(self, n: int = 1) -> None:
        self.refcount += n

    def decref(self, n: int = 1) -> int:
        refcount = self.refcount = max(self.refcount - n, 0)
        return refcount

    @classmethod
    def from_message(cls, message: Any, tp: TP) -> 'Message':
        return cls(
            message.topic,
            message.partition,
            message.offset,
            message.timestamp,
            message.timestamp_type,
            message.headers,
            message.key,
            message.value,
            message.checksum,
            message.serialized_key_size,
            message.serialized_value_size,
            tp,
        )

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self.tp} offset={self.offset}>'


class ConsumerMessage(Message):
    """Message type used by Kafka Consumer."""

    use_tracking = True

    def on_final_ack(self, consumer: _ConsumerT) -> bool:
        return consumer.ack(self)


def tp_set_to_map(tps: Set[TP]) -> MutableMapping[str, Set[TP]]:
    # convert revoked/assigned to mapping of topic to partitions
    tpmap: MutableMapping[str, Set[TP]] = defaultdict(set)
    for tp in tps:
        tpmap[tp.topic].add(tp)
    return tpmap


# XXX See top of module! We redefine this with final FutureMessage
# for Sphinx as it cannot read non-final types.
MessageSentCallback = Callable[  # type: ignore
    [FutureMessage], Union[None, Awaitable[None]]]
