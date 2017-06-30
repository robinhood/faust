"""RocksDB storage."""
from typing import Any, Iterator, Tuple
from . import base
from ..types.app import AppT
from ..utils.logging import get_logger

try:
    import rocksdb
except ImportError:
    rocksdb = None  # noqa

logger = get_logger(__name__)


class Store(base.SerializedStore):
    logger = logger

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

    def _contains(self, key: bytes) -> bool:
        return self.db.key_may_exist(key)[0]

    def _size(self) -> int:
        it = self.db.iterkeys()  # noqa: B301
        it.seek_to_first()
        return sum(1 for _ in it)

    def _iterkeys(self) -> Iterator[bytes]:
        it = self.db.iterkeys()  # noqa: B301
        it.seek_to_first()
        yield from it

    def _itervalues(self) -> Iterator[bytes]:
        it = self.db.itervalues()  # noqa: B301
        it.seek_to_first()
        yield from it

    def _iteritems(self) -> Iterator[Tuple[bytes, bytes]]:
        it = self.db.iteritems()  # noqa: B301
        it.seek_to_first()
        yield from it

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

    def _clear(self) -> None:
        # XXX
        raise NotImplementedError('TODO')

    @property
    def db(self) -> rocksdb.DB:
        if self._db is None:
            self._db = self._open_db()
        return self._db

    @property
    def filename(self) -> str:
        name = self.url.partition('://')[-1]
        return f'{name}.db' if '.' not in name else name
