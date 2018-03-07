import abc
from typing import MutableMapping, Optional, Tuple, Type

from .events import EventT
from .models import FieldDescriptorT, ModelT
from .streams import JoinableT

__all__ = ['JoinT']


class JoinT(abc.ABC):
    fields: MutableMapping[Type[ModelT], FieldDescriptorT]
    stream: JoinableT

    @abc.abstractmethod
    def __init__(self, *, stream: JoinableT,
                 fields: Tuple[FieldDescriptorT, ...]) -> None:
        ...

    @abc.abstractmethod
    async def process(self, event: EventT) -> Optional[EventT]:
        ...
