"""RocksDB storage."""
import shutil
from contextlib import suppress
from pathlib import Path
from typing import (
    Any, Callable, Iterable, Iterator, Mapping,
    MutableMapping, Optional, Tuple, Union,
)
from yarl import URL
from . import base
from ..types import AppT, EventT, TopicPartition

try:
    import rocksdb
except ImportError:
    rocksdb = None  # noqa


class CheckpointWriteBusy(Exception):
    """Raised when another instance has the checkpoint db open for writing."""


class RocksDBOptions:
    max_open_files: int = 300000
    write_buffer_size: int = 67108864
    max_write_buffer_number: int = 3
    target_file_size_base: int = 67108864
    block_cache_size: int = 2 * 1024 ** 3
    block_cache_compressed_size: int = 500 * 1024 ** 2
    bloom_filter_size: int = 3
    extra_options: Mapping

    def __init__(self,
                 max_open_files: int = None,
                 write_buffer_size: int = None,
                 max_write_buffer_number: int = None,
                 target_file_size_base: int = None,
                 block_cache_size: int = None,
                 block_cache_compressed_size: int = None,
                 bloom_filter_size: int = None,
                 **kwargs: Any) -> None:
        if max_open_files is not None:
            self.max_open_files = max_open_files
        if write_buffer_size is not None:
            self.write_buffer_size = write_buffer_size
        if max_write_buffer_number is not None:
            self.max_write_buffer_number = max_write_buffer_number
        if target_file_size_base is not None:
            self.target_file_size_base = target_file_size_base
        if block_cache_size is not None:
            self.block_cache_size = block_cache_size
        if block_cache_compressed_size is not None:
            self.block_cache_compressed_size = block_cache_compressed_size
        self.extra_options = kwargs

    def open(self, path: Path, *, read_only: bool = False) -> rocksdb.DB:
        return rocksdb.DB(str(path), self.as_options(), read_only=read_only)

    def as_options(self) -> rocksdb.Options:
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
            **self.extra_options)


class CheckpointDBOptions(RocksDBOptions):
    write_buffer_size: int = 3000
    max_write_buffer_number: int = 2
    target_file_size_base: int = 8864
    block_cache_size: int = 262144
    block_cache_compressed_size: int = 262144 * 2


class CheckpointDB:

    # The checkpoint DB is not a singleton, but whatever table
    # is used first, will create one and set an App._rocksdb_checkpoints
    # attribute that is reused by all tables.

    options: CheckpointDBOptions

    tp_separator: str = '//'

    # RocksDB database files do not allow concurrent access from multiple
    # processes.

    # when we first start, we import tps and offsets from a readonly
    # DB instance to _imported.
    _imported: MutableMapping[TopicPartition, int]
    _offsets_imported = False

    # later as changelogs are set by tables, the in-memory _offsets
    # dict will be updated.
    _offsets: MutableMapping[TopicPartition, int]

    # the dirty flag is set whenever a tp is updated, that way
    # when the Tables shutdown, stopping their Store, the store
    # will call Checkpoints.sync() to write the contents of _offsets
    # to the db.
    #
    # The _dirty flag also helps controlling what happens when 20 tables
    # all shutdown and call sync: only the first will sync, the rest will
    # sync only if the offsets have changed for some reason.
    _dirty: bool = False

    def __init__(self, app: AppT,
                 *,
                 options: Mapping = None) -> None:
        self.app = app
        self.options = CheckpointDBOptions(**options or {})
        self._imported = {}
        self._offsets = {}

    def reset_state(self) -> None:
        self._db = None
        with suppress(FileNotFoundError):
            shutil.rmtree(self.path.absolute())

    def get_offset(self, tp: TopicPartition) -> Optional[int]:
        try:
            # check in-memory copy first.
            return self._offsets[tp]
        except KeyError:
            # then check the checkpoints imported from the db.
            if not self._offsets_imported:
                self._offsets_imported = True
                self._import()
            try:
                return self._imported[tp]
            except KeyError:
                return None

    def _import(self) -> None:
        db = self.options.open(self.path, read_only=True)
        cursor = db.iteritems()  # noqa: B301
        cursor.seek_to_first()
        self._imported.update({
            self._bytes_to_tp(key): self._bytes_to_offset(value)
            for key, value in cursor
        })

    def set_offset(self, tp: TopicPartition, offset: int) -> None:
        self._offsets[tp] = offset
        self._dirty = True

    def sync(self) -> None:
        if self._dirty:
            self._dirty = False
            self._export()

    def _export(self) -> None:
        try:
            db = self.options.open(self.path, read_only=False)
        except rocksdb.errors.RocksIOError:
            raise CheckpointWriteBusy()
        else:
            w = rocksdb.WriteBatch()
            for tp, offset in self._offsets.items():
                w.put(self._tp_to_bytes(tp), self._offset_to_bytes(offset))
            db.write(w, sync=True)

    def _offset_to_bytes(self, offset: int) -> bytes:
        # bytes(num) gives zero-padded bytes of num length
        return str(offset).encode()

    def _bytes_to_offset(self, offset: bytes) -> int:
        return int(offset.decode())

    def _tp_to_bytes(self, tp: TopicPartition) -> bytes:
        return f'{tp.topic}{self.tp_separator}{tp.partition}'.encode()

    def _bytes_to_tp(self, tp: bytes) -> TopicPartition:
        topic, _, partition = tp.decode().rpartition(self.tp_separator)
        return TopicPartition(topic, int(partition))

    @property
    def filename(self) -> Path:
        return Path('__checkpoints.db')

    @property
    def path(self) -> Path:
        return self.app.tabledir / self.filename


class Store(base.SerializedStore):

    _db: rocksdb.DB = None
    options: RocksDBOptions

    def __init__(self, url: Union[str, URL], app: AppT,
                 *,
                 options: Mapping = None,
                 **kwargs: Any) -> None:
        super().__init__(url, app, **kwargs)
        if not self.url.path:
            self.url /= self.table_name
        self.options = RocksDBOptions(**options or {})
        self._db = None

    def persisted_offset(self, tp: TopicPartition) -> Optional[int]:
        return self.checkpoints.get_offset(tp)

    def set_persisted_offset(self, tp: TopicPartition, offset: int) -> None:
        self.checkpoints.set_offset(tp, offset)

    def apply_changelog_batch(self, batch: Iterable[EventT],
                              to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        w = rocksdb.WriteBatch()
        for event in batch:
            w.put(event.message.key, event.message.value)
        self.db.write(w)

    def _get(self, key: bytes) -> bytes:
        return self.db.get(key)

    def _set(self, key: bytes, value: bytes) -> None:
        self.db.put(key, value)

    def _del(self, key: bytes) -> None:
        self.db.delete(key)

    def _contains(self, key: bytes) -> bool:
        # bloom filter: false positives possible, but not false negatives
        db = self.db
        if db.key_may_exist(key)[0]:
            return db.get(key) is not None
        return False

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

    def _clear(self) -> None:
        # XXX
        raise NotImplementedError('TODO')

    def reset_state(self) -> None:
        self._db = None
        with suppress(FileNotFoundError):
            shutil.rmtree(self.path.absolute())

    @property
    def db(self) -> rocksdb.DB:
        if self._db is None:
            self._db = self.options.open(self.path)
        return self._db

    @property
    def filename(self) -> Path:
        return Path(self.url.path).with_suffix('.db')

    @property
    def path(self) -> Path:
        return self.app.tabledir / self.filename

    async def on_stop(self) -> None:
        while 1:
            try:
                self.checkpoints.sync()
            except CheckpointWriteBusy:
                self.log.info('Checkpoint lock held, retry in 1s...')
                await self.sleep(1.0)
            else:
                self.log.info('Checkpoints written to disk')
                break

    @property
    def checkpoints(self) -> CheckpointDB:
        # We cache this on the app, so that the CheckpointDB is shared
        # between all Rocksdb Store instances.
        try:
            return self.app._rocksdb_checkpoints
        except AttributeError:
            cp = self.app._rocksdb_checkpoints = CheckpointDB(self.app)
            return cp
