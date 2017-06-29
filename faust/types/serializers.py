import abc
import typing
from typing import Any, Mapping, Optional
from .codecs import CodecArg
from .core import K, V
from ..utils.imports import SymbolArg

if typing.TYPE_CHECKING:
    from .app import AppT
    from .models import ModelArg, ModelT
else:
    class AppT: ...      # noqa
    class ModelArg: ...  # noqa
    class ModelT: ...    # noqa

__all__ = ['AsyncSerializerT', 'RegistryT']


class AsyncSerializerT(abc.ABC):
    app: AppT

    @abc.abstractmethod
    def __init__(self, app: AppT) -> None:
        ...

    @abc.abstractmethod
    async def loads(self, s: bytes) -> Any:
        ...

    @abc.abstractmethod
    async def dumps_key(self, topic: str, s: ModelT) -> bytes:
        ...

    @abc.abstractmethod
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
    async def loads_key(
            self,
            typ: Optional[ModelArg],
            key: bytes) -> K:
        ...

    @abc.abstractmethod
    async def loads_value(
            self,
            typ: ModelArg,
            value: bytes) -> Any:
        ...

    @abc.abstractmethod
    async def dumps_key(self, topic: str, key: K,
                        serializer: CodecArg = None) -> Optional[bytes]:
        ...

    @abc.abstractmethod
    async def dumps_value(self, topic: str, value: V,
                          serializer: CodecArg = None) -> Optional[bytes]:
        ...
