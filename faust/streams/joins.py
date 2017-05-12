"""Join strategies."""
from typing import Optional, Tuple
from ..types import Event, FieldDescriptorT, JoinT, JoinableT

__all__ = [
    'Join',
    'RightJoin', 'LeftJoin', 'InnerJoin', 'OuterJoin',
]


class Join(JoinT):

    def __init__(self, *,
                 stream: JoinableT,
                 fields: Tuple[FieldDescriptorT, ...]) -> None:
        self.fields = {field.model: field for field in fields}
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
