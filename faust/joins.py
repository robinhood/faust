from typing import Optional, Tuple
from .types import FieldDescriptorT, JoinT, StreamT, V


class Join(JoinT):

    def __init__(self, *,
                 stream: StreamT,
                 fields: Tuple[FieldDescriptorT, ...]) -> None:
        self.fields = {field.event: field for field in fields}
        self.stream = stream

    async def process(self, event: V) -> Optional[V]:
        raise NotImplementedError()


class RightJoin(Join):
    ...


class LeftJoin(Join):
    ...


class InnerJoin(Join):
    ...


class OuterJoin(Join):
    ...
