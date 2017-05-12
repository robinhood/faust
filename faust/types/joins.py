import abc
from typing import MutableMapping, Optional, Type
from .models import FieldDescriptorT, Event
from .streams import JoinableT

__all__ = ['JoinT']


class JoinT(metaclass=abc.ABCMeta):
    fields: MutableMapping[Type, FieldDescriptorT]
    stream: JoinableT

    @abc.abstractmethod
    async def process(_self, event: Event) -> Optional[Event]:
        ...
