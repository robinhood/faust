import typing
from typing import MutableMapping, Optional, Tuple, Type
from .event import Event, FieldDescriptor

if typing.TYPE_CHECKING:
    from .streams import Stream
else:
    class Stream: ...  # noqa


class Join:
    fields: MutableMapping[Type, FieldDescriptor]

    def __init__(self, *,
                 stream: Stream,
                 fields: Tuple[FieldDescriptor, ...]) -> None:
        self.fields = {field.event: field for field in fields}
        self.stream = stream

    def __call__(self, event: Event) -> Optional[Event]:
        raise NotImplementedError()


class RightJoin(Join):
    ...


class LeftJoin(Join):
    ...


class InnerJoin(Join):
    ...


class OuterJoin(Join):
    ...
