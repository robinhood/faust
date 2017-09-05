import sys
from typing import Any, Optional, Tuple, Type, cast
from .codecs import CodecArg, CodecT, dumps, loads
from ..exceptions import KeyDecodeError, ValueDecodeError
from ..types import K, ModelArg, ModelT, V
from ..types.serializers import RegistryT
from ..utils.compat import want_bytes
from ..utils.objects import cached_property

__all__ = ['Registry']

IsInstanceArg = Tuple[Type, ...]


class Registry(RegistryT):

    def __init__(self,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = 'json') -> None:
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    def loads_key(self, typ: Optional[ModelArg], key: bytes) -> K:
        """Deserialize message key.

        Arguments:
            typ: Model to use for deserialization.
            key: Serialized key.
        """
        if key is None or typ is None:
            return key
        try:
            if typ is None or isinstance(typ, (str, CodecT)):
                k = self.Model._maybe_reconstruct(
                    self._loads(self.key_serializer, key))
            else:
                k = self._loads_model(
                    cast(Type[ModelT], typ), self.key_serializer, key)
            return cast(K, k)
        except Exception as exc:
            raise KeyDecodeError(
                str(exc)).with_traceback(sys.exc_info()[2]) from None

    def _loads_model(
            self,
            typ: Type[ModelT],
            default_serializer: CodecArg,
            data: bytes) -> Any:
        data = self._loads(
            typ._options.serializer or default_serializer, data)
        self_cls = self.Model._maybe_namespace(data)
        return self_cls(data) if self_cls else typ(data)

    def _loads(self, serializer: CodecArg, data: bytes) -> Any:
        return loads(serializer, data)

    def loads_value(self, typ: ModelArg, value: bytes) -> Any:
        """Deserialize value.

        Arguments:
            typ: Model to use for deserialization.
            value: Bytestring to deserialize.
        """
        if value is None:
            return None
        try:
            serializer = self.value_serializer
            if typ is None or isinstance(typ, (str, CodecT)):
                return self.Model._maybe_reconstruct(
                    self._loads(serializer, value))
            else:
                return self._loads_model(
                    cast(Type[ModelT], typ), serializer, value)
        except Exception as exc:
            raise ValueDecodeError(
                str(exc)).with_traceback(sys.exc_info()[2]) from None

    def dumps_key(self, key: K,
                  serializer: CodecArg = None,
                  *,
                  skip: IsInstanceArg = (bytes,)) -> Optional[bytes]:
        """Serialize key.

        Arguments:
            key: The key to be serialized.
            serializer: Custom serializer to use if value is not a Model.
        """
        serializer = self.key_serializer
        is_model = False
        if isinstance(key, ModelT):
            is_model = True
            key = cast(ModelT, key)
            serializer = key._options.serializer or serializer

        if serializer and not isinstance(key, skip):
            if is_model:
                return cast(ModelT, key).dumps(serializer=serializer)
            return dumps(serializer, key)
        return want_bytes(cast(bytes, key)) if key is not None else None

    def dumps_value(self, value: V,
                    serializer: CodecArg = None) -> Optional[bytes]:
        """Serialize value.

        Arguments:
            value: The value to be serialized.
            serializer: Custom serializer to use if value is not a Model.
        """
        is_model = False
        if isinstance(value, ModelT):
            is_model = True
            value = cast(ModelT, value)
            serializer = value._options.serializer or self.value_serializer
        if serializer:
            if is_model:
                return cast(ModelT, value).dumps(serializer=serializer)
            return dumps(serializer, value)
        return cast(bytes, value)

    @cached_property
    def Model(self) -> Type[ModelT]:
        from ..models.base import Model
        return Model


__flake8_Any_is_really_used: Any  # XXX flake8 bug
