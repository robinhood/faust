"""RocksDB storage."""
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Tuple
from . import base
from ..types import AppT, EventT, TopicPartition
from ..utils.logging import get_logger

try:
    import rocksdb
except ImportError:
    rocksdb = None  # noqa

logger = get_logger(__name__)


class Store(base.SerializedStore):
    logger = logger

    _db: rocksdb.DB = None
    _dirty: Dict

    offset_key_prefix = '__FAUST-OFFSET-{topic}-{partition}'

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
        self._dirty = {}
        self._db = None

    def _offset_key(self, tp: TopicPartition) -> bytes:
        return self.offset_key_prefix.format(
            topic=tp.topic, partition=tp.partition).encode()

    def on_changelog_sent(self, tp: TopicPartition, offset: int,
                          key: bytes, value: bytes) -> None:
        offset_key = self._offset_key(tp)
        cur_offset_bytes = self.db.get(offset_key)
        if cur_offset_bytes is not None:
            cur_offset = self._decode_value(cur_offset_bytes)
            if offset > cur_offset:
                batch = rocksdb.WriteBatch()
                batch.put(key, value)
                batch.put(offset_key, self._encode_value(offset))
                self.db.write(batch)
                return None
        self.db.put(key, value)

    def persisted_offset(self, tp: TopicPartition) -> Optional[int]:
        val = self.db.get(self._offset_key(tp))
        return self._decode_value(val) if val is not None else None

    def apply_changelog_batch(self, batch: Iterable[EventT],
                              to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        offsets: Dict[TopicPartition, int] = {}
        w = rocksdb.WriteBatch()
        for event in batch:
            tp = event.message.tp
            msg_offset, cur_offset = event.message.offset, offsets.get(tp)
            if cur_offset is None or cur_offset < msg_offset:
                offsets[tp] = msg_offset
            w.put(event.message.key, event.message.value)
        for tp, offset in offsets.items():
            w.put(self._offset_key(tp), self._encode_value(offset))
        self.db.write(w)

    def _get(self, key: bytes) -> bytes:
        dirty = self._dirty
        if key in dirty:
            return dirty[key]
        return self.db.get(key)

    def _set(self, key: bytes, value: bytes) -> None:
        self._dirty[key] = value
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
