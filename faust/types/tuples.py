import typing
from typing import NamedTuple, Pattern, Sequence, Type
from .core import K

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa

__all__ = ['Topic', 'Message', 'Request']


class Topic(NamedTuple):
    topics: Sequence[str]
    pattern: Pattern
    key_type: Type
    value_type: Type

    def __repr__(self) -> str:  # type: ignore
        return '<{}: {}>'.format(
            type(self).__name__,
            (self.pattern.pattern if self.pattern
             else ', '.join(map(repr, self.topics))),
        )


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
    app: AppT
    key: K
    message: Message
