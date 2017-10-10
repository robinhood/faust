"""RocksDB storage."""
import random
import shutil
import typing
from collections import defaultdict
from contextlib import suppress
from pathlib import Path
from typing import (
    Any, Callable, DefaultDict, Iterable, Iterator, Mapping,
    MutableMapping, NamedTuple, Optional, Tuple, Union, cast,
)
from mode import Seconds, Service, want_seconds
from yarl import URL
from . import base
from ..streams import current_event
from ..types import AppT, CollectionT, EventT, TP
from ..utils.collections import LRUCache

try:
    import rocksdb
except ImportError:
    rocksdb = None  # noqa


if typing.TYPE_CHECKING:
    from rocksdb import DB, Options
else:
    class DB: ...  # noqa
    class Options: ...  # noqa


class CheckpointWriteBusy(Exception):
    'Raised when another instance has the checkpoint db open for writing.'


class PartitionDB(NamedTuple):
    partition: int
    db: DB


class _DBValueTuple(NamedTuple):
    db: DB
    value: bytes


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

    def open(self, path: Path, *, read_only: bool = False) -> DB:
        return rocksdb.DB(str(path), self.as_options(), read_only=read_only)

    def as_options(self) -> Options:
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
    _imported: MutableMapping[TP, int]
    _offsets_imported = False

    # later as changelogs are set by tables, the in-memory _offsets
    # dict will be updated.
    _offsets: MutableMapping[TP, int]

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

    def get_offset(self, tp: TP) -> Optional[int]:
        try:
            # check in-memory copy first.
            return self._offsets[tp]
        except KeyError:
            # then check the checkpoints imported from the db.
            if not self._offsets_imported and (self.path / 'CURRENT').exists():
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

    def set_offset(self, tp: TP, offset: int) -> None:
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

    def _tp_to_bytes(self, tp: TP) -> bytes:
        return f'{tp.topic}{self.tp_separator}{tp.partition}'.encode()

    def _bytes_to_tp(self, tp: bytes) -> TP:
        topic, _, partition = tp.decode().rpartition(self.tp_separator)
        return TP(topic, int(partition))

    @property
    def filename(self) -> Path:
        return Path('__checkpoints.db')

    @property
    def path(self) -> Path:
        return self.app.tabledir / self.filename


class Store(base.SerializedStore):

    #: How often we write offsets to the checkpoints db (30s).
    sync_frequency: float

    #: Decides the size of the K=>TopicPartition index (10_000).
    key_index_size: int

    #: The checkpoints file can only be open by one process at a time,
    #: and contention occurs if many nodes attempt it at the same time,
    #: so we skew the frequency by ``gauss(freq, max_ratio)``, where
    #: ratio by default is one quarter of the frequency (0.25).
    #: For example if the frequency is 30.0, then the skew can
    #: be +/-7.5 seconds at most.
    sync_frequency_skew_max_ratio: float

    #: How long to wait before retrying when another instance
    #: is writing to the checkpoints db (1s).
    sync_locked_wait_max: float

    #: Used to configure the RocksDB settings for table stores.
    options: RocksDBOptions

    _dbs: MutableMapping[int, DB] = None
    _key_index: LRUCache[bytes, int]

    def __init__(self, url: Union[str, URL], app: AppT,
                 *,
                 sync_frequency: Seconds = 30.0,
                 sync_frequency_skew_max_ratio: float = 0.25,
                 sync_locked_wait_max: Seconds = 1.0,
                 key_index_size: int = 10_000,
                 options: Mapping = None,
                 **kwargs: Any) -> None:
        super().__init__(url, app, **kwargs)
        if not self.url.path:
            self.url /= self.table_name
        self.options = RocksDBOptions(**options or {})
        self.sync_frequency = want_seconds(sync_frequency)
        self.sync_frequency_skew_max_ratio = sync_frequency_skew_max_ratio
        self.sync_locked_wait_max = want_seconds(sync_locked_wait_max)
        self.key_index_size = key_index_size
        self._dbs = {}
        self._key_index = LRUCache(limit=self.key_index_size)

    def persisted_offset(self, tp: TP) -> Optional[int]:
        return self.checkpoints.get_offset(tp)

    def set_persisted_offset(self, tp: TP, offset: int) -> None:
        self.checkpoints.set_offset(tp, offset)

    async def need_active_standby_for(self, tp: TP) -> bool:
        try:
            self._db_for_partition(tp.partition)
        except rocksdb.errors.RocksIOError as exc:
            if 'lock' not in repr(exc):
                raise
            return False
        else:
            return True

    def apply_changelog_batch(self, batch: Iterable[EventT],
                              to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        batches: DefaultDict[int, rocksdb.WriteBatch]
        batches = defaultdict(rocksdb.WriteBatch)
        for event in batch:
            msg = event.message
            batches[msg.partition].put(msg.key, msg.value)

        for partition, batch in batches.items():
            self._db_for_partition(partition).write(batch)

    def _set(self, key: bytes, value: bytes) -> None:
        event = current_event()
        assert event is not None
        partition = event.message.partition
        db = self._db_for_partition(partition)
        self._key_index[key] = partition
        db.put(key, value)

    def _db_for_partition(self, partition: int) -> DB:
        try:
            return self._dbs[partition]
        except KeyError:
            db = self._dbs[partition] = self._open_for_partition(partition)
            return db

    def _open_for_partition(self, partition: int) -> DB:
        return self.options.open(self.partition_path(partition))

    def _get(self, key: bytes) -> bytes:
        dbvalue = self._get_bucket_for_key(key)
        if dbvalue is None:
            return None
        db, value = dbvalue

        if value is None:
            if db.key_may_exist(key)[0]:
                value = db.get(key)
                if value is not None:
                    return value
        return value

    def _get_bucket_for_key(self, key: bytes) -> Optional[_DBValueTuple]:
        dbs: Iterable[PartitionDB]
        try:
            partition = self._key_index[key]
            dbs = [PartitionDB(partition, self._dbs[partition])]
        except KeyError:
            dbs = cast(Iterable[PartitionDB], self._dbs.items())

        for partition, db in dbs:
            if db.key_may_exist(key)[0]:
                value = db.get(key)
                if value is not None:
                    self._key_index[key] = partition
                    return _DBValueTuple(db, value)
        return None

    def _del(self, key: bytes) -> None:
        for db in self._dbs_for_key(key):
            db.delete(key)

    async def on_partitions_revoked(
            self,
            table: CollectionT,
            revoked: Iterable[TP]) -> None:
        for tp in revoked:
            if tp.topic in table.changelog_topic.topics:
                db = self._dbs.pop(tp.partition, None)
                if db is not None:
                    del(db)
        import gc
        gc.collect()  # XXX RocksDB has no .close() method :X
        self._key_index.clear()

    async def on_partitions_assigned(
            self,
            table: CollectionT,
            assigned: Iterable[TP]) -> None:
        self._key_index.clear()
        standby_tps = self.app.assignor.assigned_standbys()
        my_topics = table.changelog_topic.topics

        for tp in assigned:
            if tp.topic in my_topics and tp not in standby_tps:
                for i in range(5):
                    try:
                        # side effect: opens db and adds to self._dbs.
                        self._db_for_partition(tp.partition)
                    except rocksdb.errors.RocksIOError as exc:
                        if i == 4 or 'lock' not in repr(exc):
                            raise
                        self.log.info(
                            'DB for partition %r is locked! Retry in 1s...',
                            tp.partition)
                        await self.sleep(1.0)
                    else:
                        break

    def _contains(self, key: bytes) -> bool:
        for db in self._dbs_for_key(key):
            # bloom filter: false positives possible, but not false negatives
            if db.key_may_exist(key)[0] and db.get(key) is not None:
                return True
        return False

    def _dbs_for_key(self, key: bytes) -> Iterable[DB]:
        # Returns cached db if key is in index, otherwise all dbs
        # for linear search.
        try:
            return [self._dbs[self._key_index[key]]]
        except KeyError:
            return self._dbs.values()

    def _size(self) -> int:
        return sum(self._size1(db) for db in self._dbs.values())

    def _size1(self, db: DB) -> int:
        it = db.iterkeys()  # noqa: B301
        it.seek_to_first()
        return sum(1 for _ in it)

    def _iterkeys(self) -> Iterator[bytes]:
        for db in self._dbs.values():
            it = db.iterkeys()  # noqa: B301
            it.seek_to_first()
            yield from it

    def _itervalues(self) -> Iterator[bytes]:
        for db in self._dbs.values():
            it = db.itervalues()  # noqa: B301
            it.seek_to_first()
            yield from it

    def _iteritems(self) -> Iterator[Tuple[bytes, bytes]]:
        for db in self._dbs.values():
            it = db.iteritems()  # noqa: B301
            it.seek_to_first()
            yield from it

    def _clear(self) -> None:
        # XXX
        raise NotImplementedError('TODO')

    def reset_state(self) -> None:
        self._dbs.clear()
        self._key_index.clear()
        with suppress(FileNotFoundError):
            shutil.rmtree(self.path.absolute())

    def partition_path(self, partition: int) -> Path:
        p = self.path / self.basename
        return self.with_suffix(p.with_name(f'{p.name}-{partition}'))

    def with_suffix(self, path: Path, *, suffix: str = '.db') -> Path:
        return path.with_suffix(suffix)

    @property
    def path(self) -> Path:
        return self.app.tabledir

    @property
    def basename(self) -> Path:
        return Path(self.url.path)

    async def on_stop(self) -> None:
        await self.sync_checkpoints()

    @Service.task
    async def _background_sync_checkpoints(self) -> None:
        while not self.should_stop:
            await self.sleep(random.gauss(
                self.sync_frequency, self.sync_frequency_skew_max_ratio))
            await self.sync_checkpoints()

    async def sync_checkpoints(self) -> None:
        while 1:
            self.log.info('Flush checkpoints to disk...')
            try:
                self.checkpoints.sync()
            except CheckpointWriteBusy:
                self.log.info('Checkpoint lock held, retry in %ss...',
                              self.sync_locked_wait_max)
                await self.sleep(self.sync_locked_wait_max)
            else:
                self.log.debug('Checkpoints written OK!')
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


__flake8_DefaultDict_is_used: DefaultDict  # XXX flake8 bug
