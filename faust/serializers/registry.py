"""Registry of supported codecs (serializers, compressors, etc.)."""
import sys
from decimal import Decimal
from typing import Any, Optional, Tuple, Type, cast

from mode.utils.compat import want_bytes, want_str
from mode.utils.objects import cached_property

from faust.exceptions import KeyDecodeError, ValueDecodeError
from faust.types import K, ModelArg, ModelT, V
from faust.types.serializers import RegistryT

from .codecs import CodecArg, dumps, loads

__all__ = ['Registry']

IsInstanceArg = Tuple[Type, ...]


class Registry(RegistryT):
    """Serializing message keys/values.

    Arguments:
        key_serializer: Default key serializer to use when none provided.
        value_serializer: Default value serializer to use when none provided.
    """

    def __init__(self,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = 'json') -> None:
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    def loads_key(self,
                  typ: Optional[ModelArg],
                  key: Optional[bytes],
                  *,
                  serializer: CodecArg = None) -> K:
        """Deserialize message key.

        Arguments:
            typ: Model to use for deserialization.
            key: Serialized key.

            serializer: Codec to use for this value.  If not set
               the default will be used (:attr:`key_serializer`).
        """
        if key is None:
            if typ is not None and issubclass(typ, ModelT):
                raise KeyDecodeError(f'Expected {typ!r}, received {key!r}!')
            return key
        serializer = serializer or self.key_serializer
        try:
            payload = self._loads(serializer, key)
            return cast(K, self._prepare_payload(typ, payload))
        except MemoryError:
            raise
        except Exception as exc:
            raise KeyDecodeError(str(exc)).with_traceback(
                sys.exc_info()[2]) from exc

    def _loads(self, serializer: CodecArg, data: bytes) -> Any:
        return loads(serializer, data)

    def _serializer(self, typ: Optional[ModelArg], *alt: CodecArg) -> CodecArg:
        serializer = None
        for serializer in alt:
            if serializer:
                break
        else:
            if typ is str:
                return 'raw'
            elif typ is bytes:
                return 'raw'
        return serializer

    def loads_value(self,
                    typ: Optional[ModelArg],
                    value: Optional[bytes],
                    *,
                    serializer: CodecArg = None) -> Any:
        """Deserialize value.

        Arguments:
            typ: Model to use for deserialization.
            value: Bytestring to deserialize.

            serializer: Codec to use for this value.  If not set
               the default will be used (:attr:`value_serializer`).
        """
        if value is None:
            if typ is not None and issubclass(typ, ModelT):
                raise ValueDecodeError(
                    f'Expected {typ!r}, received {value!r}!')
            return None
        serializer = self._serializer(typ, serializer, self.value_serializer)
        try:
            payload = self._loads(serializer, value)
            return cast(V, self._prepare_payload(typ, payload))
        except MemoryError:
            raise
        except Exception as exc:
            raise ValueDecodeError(str(exc)).with_traceback(
                sys.exc_info()[2]) from exc

    def _prepare_payload(self, typ: Optional[ModelArg], value: Any) -> Any:
        if typ is None:  # (autodetect)
            return self.Model._maybe_reconstruct(value)
        elif typ is int:
            return int(want_str(value))
        elif typ is float:
            return float(want_str(value))
        elif typ is Decimal:
            return Decimal(want_str(value))
        elif typ is str:
            return want_str(value)
        elif typ is bytes:
            return want_bytes(value)
        else:
            # type set to Model
            model = cast(ModelT, typ)
            return model.from_data(value, preferred_type=model)

    def dumps_key(self,
                  typ: Optional[ModelArg],
                  key: K,
                  *,
                  serializer: CodecArg = None,
                  skip: IsInstanceArg = (bytes,)) -> Optional[bytes]:
        """Serialize key.

        Arguments:
            typ: Model hint (can also be str/bytes).
                 When `typ=str` or `bytes`, raw serializer is assumed.
            key: The key value to serializer.

            serializer: Codec to use for this key, if it is not a model type.
               If not set the default will be used (:attr:`key_serializer`).
        """
        is_model = False
        if isinstance(key, ModelT):
            is_model = True
            key = cast(ModelT, key)
            serializer = key._options.serializer or serializer
        serializer = self._serializer(typ, serializer, self.key_serializer)
        if serializer and not isinstance(key, skip):
            if is_model:
                return cast(ModelT, key).dumps(serializer=serializer)
            return dumps(serializer, key)
        return want_bytes(cast(bytes, key)) if key is not None else None

    def dumps_value(self,
                    typ: Optional[ModelArg],
                    value: V,
                    *,
                    serializer: CodecArg = None,
                    skip: IsInstanceArg = (bytes,)) -> Optional[bytes]:
        """Serialize value.

        Arguments:
            typ: Model hint (can also be str/bytes).
                 When `typ=str` or `bytes`, raw serializer is assumed.
            key: The value to serializer.

            serializer: Codec to use for this value, if it is not a model type.
              If not set the default will be used (:attr:`value_serializer`).
        """
        is_model = False
        if isinstance(value, ModelT):
            is_model = True
            value = cast(ModelT, value)
            serializer = value._options.serializer or serializer
        serializer = self._serializer(typ, serializer, self.value_serializer)
        if serializer and not isinstance(value, skip):
            if is_model:
                return cast(ModelT, value).dumps(serializer=serializer)
            return dumps(serializer, value)
        return cast(bytes, value)

    @cached_property
    def Model(self) -> Type[ModelT]:
        from faust.models.base import Model
        return Model
