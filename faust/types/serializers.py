import abc
import typing
from typing import Any, Optional

from .codecs import CodecArg
from .core import K, V

if typing.TYPE_CHECKING:
    from .models import ModelArg as _ModelArg
else:
    class _ModelArg: ...  # noqa

__all__ = ['RegistryT']


class RegistryT(abc.ABC):

    key_serializer: CodecArg
    value_serializer: CodecArg

    @abc.abstractmethod
    def __init__(self,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = 'json') -> None:
        ...

    @abc.abstractmethod
    def loads_key(self,
                  typ: Optional[_ModelArg],
                  key: Optional[bytes],
                  *,
                  serializer: CodecArg = None) -> K:
        ...

    @abc.abstractmethod
    def loads_value(self,
                    typ: Optional[_ModelArg],
                    value: Optional[bytes],
                    *,
                    serializer: CodecArg = None) -> Any:
        ...

    @abc.abstractmethod
    def dumps_key(self,
                  typ: Optional[_ModelArg],
                  key: K,
                  *,
                  serializer: CodecArg = None) -> Optional[bytes]:
        ...

    @abc.abstractmethod
    def dumps_value(self,
                    typ: Optional[_ModelArg],
                    value: V,
                    *,
                    serializer: CodecArg = None) -> Optional[bytes]:
        ...
