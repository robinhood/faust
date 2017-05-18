import typing
from typing import Any, NamedTuple, Sequence, Union
from weakref import WeakSet  # type: ignore
from .codecs import CodecArg
from .core import K, V

if typing.TYPE_CHECKING:
    from .app import AppT
    from .topics import SourceT, TopicT
else:
    class AppT: ...      # noqa
    class SourceT: ...   # noqa
    class TopicT: ...    # noqa

__all__ = ['TopicPartition', 'Message']


class TopicPartition(NamedTuple):
    topic: str
    partition: int


class PendingMessage(NamedTuple):
    topic: Union[str, TopicT]
    key: K
    value: V
    partition: int
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
        'refcount',
        'sources',
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
            self.sources: WeakSet[SourceT] = WeakSet()
        else:
            self.sources = WeakSet()

    def incref(self, source: SourceT = None, n: int = 1) -> None:
        self.sources.add(source)
        self.refcount += n

    def incref_bulk(self, sources: Sequence[SourceT]) -> None:
        self.sources.update(sources)
        self.refcount += len(sources)

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
