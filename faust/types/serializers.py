import abc
import typing
from typing import Any, Mapping, Optional, Type
from ..utils.imports import SymbolArg
from .codecs import CodecArg
from .core import K, V

if typing.TYPE_CHECKING:
    from .app import AppT
    from .models import ModelT
else:
    class AppT: ...    # noqa
    class ModelT: ...  # noqa

__all__ = ['AsyncSerializerT', 'RegistryT']


class AsyncSerializerT:
    app: AppT

    async def loads(self, s: bytes) -> Any:
        ...

    async def dumps_key(self, topic: str, s: ModelT) -> bytes:
        ...

    async def dumps_value(self, topic: str, s: ModelT) -> bytes:
        ...


class RegistryT(abc.ABC):

    override_classes: Mapping[CodecArg, SymbolArg]
    key_serializer: CodecArg
    value_serializer: CodecArg

    @abc.abstractmethod
    def __init__(self,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = 'json') -> None:
        ...

    @abc.abstractmethod
    async def loads_key(self, typ: Optional[Type], key: bytes) -> K:
        ...

    @abc.abstractmethod
    async def loads_value(self, typ: Type, value: bytes) -> Any:
        ...

    @abc.abstractmethod
    async def dumps_key(self, topic: str, key: K,
                        serializer: CodecArg = None) -> Optional[bytes]:
        ...

    @abc.abstractmethod
    async def dumps_value(self, topic: str, value: V,
                          serializer: CodecArg = None) -> Optional[bytes]:
        ...
