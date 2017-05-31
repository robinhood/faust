import abc
import typing
from typing import Any, MutableMapping
from ..utils.types.services import ServiceT
from .codecs import CodecArg

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa

__all__ = ['StoreT']


class StoreT(ServiceT, MutableMapping):

    url: str
    app: AppT
    table_name: str
    key_serializer: CodecArg
    value_serializer: CodecArg

    @abc.abstractmethod
    def __init__(self, url: str, app: AppT,
                 *,
                 table_name: str = '',
                 key_serializer: CodecArg = '',
                 value_serializer: CodecArg = '',
                 **kwargs: Any) -> None:
        ...
