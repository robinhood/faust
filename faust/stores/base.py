import abc
from typing import Any
from ..serializers.codecs import dumps, loads
from ..types import AppT, CodecArg, StoreT
from ..utils.services import Service


class Store(StoreT, Service):

    def __init__(self, url: str, app: AppT,
                 *,
                 table_name: str = '',
                 key_serializer: CodecArg = 'json',
                 value_serializer: CodecArg = 'json',
                 **kwargs: Any) -> None:
        self.url = url
        self.app = app
        self.table_name = table_name
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        super().__init__(**kwargs)

    def _encode_key(self, key: Any) -> bytes:
        return dumps(self.key_serializer, key)

    def _encode_value(self, value: Any) -> bytes:
        return dumps(self.value_serializer, value)

    def _decode_key(self, key: bytes) -> Any:
        return loads(self.key_serializer, key)

    def _decode_value(self, value: bytes) -> Any:
        return loads(self.value_serializer, value)


class SerializedStore(Store):

    @abc.abstractmethod
    def _get(self, key: bytes) -> bytes:
        ...

    @abc.abstractmethod
    def _set(self, key: bytes, value: bytes) -> None:
        ...

    @abc.abstractmethod
    def _del(self, key: bytes) -> None:
        ...

    def __getitem__(self, key: Any) -> Any:
        value = self._get(self._encode_key(key))
        if value is None:
            raise KeyError(key)
        return self._decode_value(value)

    def __setitem__(self, key: Any, value: Any) -> None:
        return self._set(self._encode_key(key), self._encode_value(value))

    def __delitem__(self, key: Any) -> None:
        return self._del(self._encode_key(key))
