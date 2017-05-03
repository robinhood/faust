from typing import MutableMapping
from ..utils.types.services import ServiceT
from .app import AppT
from .codecs import CodecArg

__all__ = ['StoreT']


class StoreT(ServiceT, MutableMapping):

    url: str
    app: AppT
    table_name: str
    key_serializer: CodecArg
    value_serializer: CodecArg
