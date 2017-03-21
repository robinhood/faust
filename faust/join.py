from typing import Optional, Tuple
from .types import EventT, FieldDescriptorT, JoinT, StreamT


class Join(JoinT):

    def __init__(self, *,
                 stream: StreamT,
                 fields: Tuple[FieldDescriptorT, ...]) -> None:
        self.fields = {field.event: field for field in fields}
        self.stream = stream

    def __call__(self, event: EventT) -> Optional[EventT]:
        raise NotImplementedError()


class RightJoin(Join):
    ...


class LeftJoin(Join):
    ...


class InnerJoin(Join):
    ...


class OuterJoin(Join):
    ...
