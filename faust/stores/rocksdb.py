"""RocksDB storage."""
from collections import ItemsView, KeysView, ValuesView
from typing import Any, Iterator, Tuple
from ..types.app import AppT
from . import base

try:
    import rocksdb
except ImportError:
    rocksdb = None  # noqa


class RocksKeysView(KeysView):

    def __init__(self, store: 'Store') -> None:
        self._mapping = store

    def __iter__(self) -> Iterator:
        yield from self._mapping


class RocksValuesView(ValuesView):

    def __init__(self, store: 'Store') -> None:
        self._mapping = store

    def __iter__(self) -> Iterator:
        yield from self._mapping._itervalues()


class RocksItemsView(ItemsView):

    def __init__(self, store: 'Store') -> None:
        self._mapping = store

    def __iter__(self) -> Iterator[Tuple[Any, Any]]:
        yield from self._mapping._iteritems()


class Store(base.SerializedStore):

    _db: rocksdb.DB = None

    def __init__(self, url: str, app: AppT,
                 *,
                 max_open_files: int = 300000,
                 write_buffer_size: int = 67108864,
                 max_write_buffer_number: int = 3,
                 target_file_size_base: int = 67108864,
                 block_cache_size: int = 2 * 1024 ** 3,
                 block_cache_compressed_size: int = 500 * 1024 ** 2,
                 bloom_filter_size: int = 10,
                 **kwargs: Any) -> None:
        super().__init__(url, app, **kwargs)
        self.max_open_files: int = max_open_files
        self.write_buffer_size: int = write_buffer_size
        self.max_write_buffer_number: int = max_write_buffer_number
        self.target_file_size_base: int = target_file_size_base
        self.block_cache_size: int = block_cache_size
        self.block_cache_compressed_size: int = block_cache_compressed_size
        self.bloom_filter_size: int = bloom_filter_size
        _, _, rest = self.url.partition('://')
        if not rest:
            self.url = self.url + self.table_name
        self._db = None

    def _get(self, key: bytes) -> bytes:
        return self.db.get(key)

    def _set(self, key: bytes, value: bytes) -> None:
        self.db.put(key, value)

    def _del(self, key: bytes) -> None:
        self.db.delete(key)

    def __iter__(self) -> Iterator:
        yield from self._iterkeys()

    def __contains__(self, key: Any) -> bool:
        return self.db.key_may_exist(self._encode_key(key))[0]

    def __len__(self) -> int:
        it = self.db.iterkeys()
        it.seek_to_first()
        return sum(1 for _ in it)

    def keys(self) -> KeysView:
        return RocksKeysView(self)

    def values(self) -> ValuesView:
        return RocksValuesView(self)

    def items(self) -> ItemsView:
        return RocksItemsView(self)

    def _iterkeys(self) -> Iterator:
        it = self.db.iterkeys()
        it.seek_to_first()
        for key in it:
            yield self._decode_key(key)

    def _itervalues(self) -> Iterator:
        it = self.db.itervalues()
        it.seek_to_first()
        for value in it:
            yield self._decode_value(value)

    def _iteritems(self) -> Iterator[Tuple[Any, Any]]:
        it = self.db.iteritems()
        it.seek_to_first()
        for key, value in it:
            yield self._decode_key(key), self._decode_value(value)

    def _open_db(self) -> rocksdb.DB:
        return rocksdb.DB(self.filename, self._options())

    def _options(self) -> rocksdb.Options:
        return rocksdb.Options(
            create_if_missing=True,
            max_open_files=self.max_open_files,
            write_buffer_size=self.write_buffer_size,
            max_write_buffer_number=self.max_write_buffer_number,
            target_file_size_base=self.target_file_size_base,
            table_factory=rocksdb.BlockBasedTableFactory(
                filter_policy=rocksdb.BloomFilterPolicy(
                    self.bloom_filter_size),
                block_cache=rocksdb.LRUCache(self.block_cache_size),
                block_cache_compressed=rocksdb.LRUCache(
                    self.block_cache_compressed_size),
            ),
        )

    @property
    def db(self) -> rocksdb.DB:
        if self._db is None:
            self._db = self._open_db()
        return self._db

    @property
    def filename(self) -> str:
        name = self.url.partition('://')[-1]
        return '{0}.db' if '.' not in name else name

    def _repr_info(self) -> str:
        return 'url={!r}'.format(self.url)
