import asyncio
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
from .core import K, V

if typing.TYPE_CHECKING:
    from .app import AppT
    from .channels import ChannelT
    from .transports import ConsumerT
else:
    class AppT: ...       # noqa
    class ChannelT: ...   # noqa
    class ConsumerT: ...  # noqa

__all__ = [
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


class PendingMessage(NamedTuple):
    channel: ChannelT
    key: K
    value: V
    partition: Optional[int]
    key_serializer: CodecArg
    value_serializer: CodecArg
    callback: Optional[MessageSentCallback]
    topic: Optional[str] = None
    offset: Optional[int] = None

    @property
    def tp(self) -> TP:
        return TP(cast(str, self.topic), cast(int, self.partition))

    def ack(self, consumer: ConsumerT) -> None:
        ...  # used as Event.message in testing


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
        '__weakref__',
    )

    def __init__(self,
                 topic: str,
                 partition: int,
                 offset: int,
                 timestamp: float,
                 timestamp_type: str,
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
        self.timestamp_type: str = timestamp_type
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
        self.tracked: bool = False

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

    def ack(self, consumer: ConsumerT, n: int = 1) -> bool:
        if not self.acked:
            # if no more references, mark offset as safe-to-commit in Consumer.
            if not self.decref(n):
                return consumer.ack(self)
        return False

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
            message.key,
            message.value,
            message.checksum,
            message.serialized_key_size,
            message.serialized_value_size,
            tp,
        )

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self.tp} offset={self.offset}>'


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
