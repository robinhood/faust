"""Join strategies."""
from typing import Optional, Tuple
from .types import Event, FieldDescriptorT, JoinT, StreamT

__all__ = [
    'Join',
    'RightJoin', 'LeftJoin', 'InnerJoin', 'OuterJoin',
]


class Join(JoinT):

    def __init__(self, *,
                 stream: StreamT,
                 fields: Tuple[FieldDescriptorT, ...]) -> None:
        self.fields = {field.event: field for field in fields}
        self.stream = stream

    async def process(self, event: Event) -> Optional[Event]:
        raise NotImplementedError()


class RightJoin(Join):
    ...


class LeftJoin(Join):
    ...


class InnerJoin(Join):
    ...


class OuterJoin(Join):
    ...
