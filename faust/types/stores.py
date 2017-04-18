from typing import MutableMapping
from ..utils.types.services import ServiceT

__all__ = ['StoreT']


class StoreT(ServiceT, MutableMapping):
    ...
