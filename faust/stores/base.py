"""Base class for table storage drivers."""
import abc
from collections import ItemsView, KeysView, ValuesView
from typing import Any, Callable, Iterable, Iterator, Optional, Tuple, Union
from mode import Service
from yarl import URL
from ..serializers.codecs import dumps, loads
from ..types import AppT, CodecArg, CollectionT, EventT, StoreT, TP


class Store(StoreT, Service):
    """Base class for table storage drivers."""

    def __init__(self, url: Union[str, URL], app: AppT,
                 *,
                 table_name: str = '',
                 key_serializer: CodecArg = 'json',
                 value_serializer: CodecArg = 'json',
                 **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.url = URL(url)
        self.app = app
        self.table_name = table_name
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    def persisted_offset(self, tp: TP) -> Optional[int]:
        raise NotImplementedError('In-memory store only, does not persist.')

    def set_persisted_offset(self, tp: TP, offset: int) -> None:
        ...

    async def need_active_standby_for(self, tp: TP) -> bool:
        return True

    async def on_partitions_assigned(
            self,
            table: CollectionT,
            assigned: Iterable[TP]) -> None:
        ...

    async def on_partitions_revoked(
            self,
            table: CollectionT,
            revoked: Iterable[TP]) -> None:
        ...

    def _encode_key(self, key: Any) -> bytes:
        return dumps(self.key_serializer, key)

    def _encode_value(self, value: Any) -> bytes:
        return dumps(self.value_serializer, value)

    def _decode_key(self, key: bytes) -> Any:
        return loads(self.key_serializer, key)

    def _decode_value(self, value: bytes) -> Any:
        return loads(self.value_serializer, value)

    def _repr_info(self) -> str:
        return f'table_name={self.table_name} url={self.url}'

    @property
    def label(self) -> str:
        return f'{type(self).__name__}: {self.table_name}'


class _SerializedStoreKeysView(KeysView):

    def __init__(self, store: 'SerializedStore') -> None:
        self._mapping = store

    def __iter__(self) -> Iterator:
        yield from self._mapping._keys_decoded()


class _SerializedStoreValuesView(ValuesView):

    def __init__(self, store: 'SerializedStore') -> None:
        self._mapping = store

    def __iter__(self) -> Iterator:
        yield from self._mapping._values_decoded()


class _SerializedStoreItemsView(ItemsView):

    def __init__(self, store: 'SerializedStore') -> None:
        self._mapping = store

    def __iter__(self) -> Iterator[Tuple[Any, Any]]:
        yield from self._mapping._items_decoded()


class SerializedStore(Store):
    """Base class for table storage drivers requiring serialization."""

    @abc.abstractmethod
    def _get(self, key: bytes) -> bytes:
        ...

    @abc.abstractmethod
    def _set(self, key: bytes, value: bytes) -> None:
        ...

    @abc.abstractmethod
    def _del(self, key: bytes) -> None:
        ...

    @abc.abstractmethod
    def _iterkeys(self) -> Iterator[bytes]:
        ...

    @abc.abstractmethod
    def _itervalues(self) -> Iterator[bytes]:
        ...

    @abc.abstractmethod
    def _iteritems(self) -> Iterator[Tuple[bytes, bytes]]:
        ...

    @abc.abstractmethod
    def _size(self) -> int:
        ...

    @abc.abstractmethod
    def _contains(self, key: bytes) -> bool:
        ...

    @abc.abstractmethod
    def _clear(self) -> None:
        ...

    def apply_changelog_batch(self, batch: Iterable[EventT],
                              to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        for event in batch:
            # keys/values are already JSON serialized in the message
            self._set(event.message.key, event.message.value)

    def __getitem__(self, key: Any) -> Any:
        value = self._get(self._encode_key(key))
        if value is None:
            raise KeyError(key)
        return self._decode_value(value)

    def __setitem__(self, key: Any, value: Any) -> None:
        return self._set(self._encode_key(key), self._encode_value(value))

    def __delitem__(self, key: Any) -> None:
        return self._del(self._encode_key(key))

    def __iter__(self) -> Iterator:
        yield from self._keys_decoded()

    def __len__(self) -> int:
        return self._size()

    def __contains__(self, key: Any) -> bool:
        return self._contains(self._encode_key(key))

    def keys(self) -> KeysView:
        return _SerializedStoreKeysView(self)

    def _keys_decoded(self) -> Iterator:
        for key in self._iterkeys():
            yield self._decode_key(key)

    def values(self) -> ValuesView:
        return _SerializedStoreValuesView(self)

    def _values_decoded(self) -> Iterator:
        for value in self._itervalues():
            yield self._decode_value(value)

    def items(self) -> ItemsView:
        return _SerializedStoreItemsView(self)

    def _items_decoded(self) -> Iterator[Tuple[Any, Any]]:
        for key, value in self._iteritems():
            yield self._decode_key(key), self._decode_value(value)

    def clear(self) -> None:
        self._clear()
