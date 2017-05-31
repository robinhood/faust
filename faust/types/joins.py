import abc
from typing import MutableMapping, Optional, Type
from .models import FieldDescriptorT, ModelT
from .streams import JoinableT
from .topics import EventT

__all__ = ['JoinT']


class JoinT(metaclass=abc.ABCMeta):
    fields: MutableMapping[Type[ModelT], FieldDescriptorT]
    stream: JoinableT

    @abc.abstractmethod
    async def process(_self, event: EventT) -> Optional[EventT]:
        ...
