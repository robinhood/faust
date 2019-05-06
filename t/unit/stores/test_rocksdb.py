from pathlib import Path
from typing import List, Mapping, Tuple
import pytest
from faust.exceptions import ImproperlyConfigured
from faust.stores import rocksdb
from faust.stores.rocksdb import RocksDBOptions, Store
from faust.types import TP
from mode.utils.mocks import AsyncMock, Mock, call, patch
from yarl import URL

TP1 = TP('foo', 0)
TP2 = TP('foo', 1)
TP3 = TP('bar', 2)
TP4 = TP('baz', 3)


class MockIterator(Mock):

    @classmethod
    def from_values(cls, values):
        it = cls()
        it.values = values
        return it

    def __iter__(self):
        return iter(self.values)


class test_RocksDBOptions:

    @pytest.mark.parametrize('arg', [
        'max_open_files',
        'write_buffer_size',
        'max_write_buffer_number',
        'target_file_size_base',
        'block_cache_size',
        'block_cache_compressed_size',
        'bloom_filter_size',
    ])
    def test_init(self, arg):
        opts = RocksDBOptions(**{arg: 30})
        assert getattr(opts, arg) == 30

    def test_defaults(self):
        opts = RocksDBOptions()
        assert opts.max_open_files == rocksdb.DEFAULT_MAX_OPEN_FILES
        assert opts.write_buffer_size == rocksdb.DEFAULT_WRITE_BUFFER_SIZE
        assert (opts.max_write_buffer_number ==
                rocksdb.DEFAULT_MAX_WRITE_BUFFER_NUMBER)
        assert (opts.target_file_size_base ==
                rocksdb.DEFAULT_TARGET_FILE_SIZE_BASE)
        assert opts.block_cache_size == rocksdb.DEFAULT_BLOCK_CACHE_SIZE
        assert (opts.block_cache_compressed_size ==
                rocksdb.DEFAULT_BLOCK_CACHE_COMPRESSED_SIZE)
        assert opts.bloom_filter_size == rocksdb.DEFAULT_BLOOM_FILTER_SIZE

    def test_open(self):
        with patch('faust.stores.rocksdb.rocksdb', Mock()) as rocks:
            opts = RocksDBOptions()
            db = opts.open(Path('foo.db'), read_only=True)
            rocks.DB.assert_called_once_with(
                'foo.db', opts.as_options(), read_only=True)
            assert db is rocks.DB()


class test_Store:

    @pytest.fixture()
    def table(self):
        table = Mock(name='table')
        table.name = 'table1'
        return table

    @pytest.yield_fixture()
    def rocks(self):
        with patch('faust.stores.rocksdb.rocksdb') as rocks:
            yield rocks

    @pytest.yield_fixture()
    def no_rocks(self):
        with patch('faust.stores.rocksdb.rocksdb', None) as rocks:
            yield rocks

    @pytest.fixture()
    def store(self, *, app, rocks, table):
        return Store('rocksdb://', app, table)

    @pytest.fixture()
    def db_for_partition(self, *, store):
        dfp = store._db_for_partition = Mock(name='db_for_partition')
        return dfp

    def test_no_rocksdb(self, *, app, table, no_rocks):
        with pytest.raises(ImproperlyConfigured):
            Store('rocksdb://', app, table)

    def test_url_without_path_adds_table_name(self, *, store):
        assert store.url == URL('rocksdb:table1')

    def test_url_having_path(self, *, app, rocks, table):
        store = Store('rocksdb://foobar/', app, table)
        assert store.url == URL('rocksdb://foobar/')

    def test_init(self, *, store):
        assert isinstance(store.options, RocksDBOptions)
        assert store.key_index_size == 10_000
        assert store._dbs == {}
        assert store._key_index is not None

    def test_persisted_offset(self, *, store, db_for_partition):
        db_for_partition.return_value.get.return_value = '300'
        assert store.persisted_offset(TP1) == 300
        db_for_partition.assert_called_once_with(TP1.partition)
        db_for_partition.return_value.get.assert_called_once_with(
            store.offset_key)

        db_for_partition.return_value.get.return_value = None
        assert store.persisted_offset(TP1) is None

    def test_set_persisted_offset(self, *, store, db_for_partition):
        store.set_persisted_offset(TP1, 3003)
        db_for_partition.assert_called_once_with(TP1.partition)
        db_for_partition.return_value.put.assert_called_once_with(
            store.offset_key, b'3003',
        )

    @pytest.mark.asyncio
    async def test_need_active_standby_for(self, *, store, db_for_partition):
        with patch('faust.stores.rocksdb.rocksdb.errors.RocksIOError',
                   KeyError):
            db_for_partition.side_effect = KeyError('lock acquired')
            assert not await store.need_active_standby_for(TP1)

    @pytest.mark.asyncio
    async def test_need_active_standby_for__raises(
            self, *, store, db_for_partition):
        with patch('faust.stores.rocksdb.rocksdb.errors.RocksIOError',
                   KeyError):
            db_for_partition.side_effect = KeyError('oh no')
            with pytest.raises(KeyError):
                await store.need_active_standby_for(TP1)

    @pytest.mark.asyncio
    async def test_need_active_standby_for__active(
            self, *, store, db_for_partition):
        with patch('faust.stores.rocksdb.rocksdb.errors.RocksIOError',
                   KeyError):
            assert await store.need_active_standby_for(TP1)

    def test_apply_changelog_batch(self, *, store, rocks, db_for_partition):

        def new_event(name, tp: TP, offset, key, value) -> Mock:
            return Mock(
                name='event1',
                message=Mock(
                    tp=tp,
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=offset,
                    key=key,
                    value=value,
                ),
            )

        events = [
            new_event('event1', TP1, 1001, 'k1', 'v1'),
            new_event('event2', TP2, 2002, 'k2', 'v2'),
            new_event('event3', TP3, 3003, 'k3', 'v3'),
            new_event('event4', TP4, 4004, 'k4', 'v4'),
            new_event('event5', TP4, 4005, 'k5', None),
        ]

        dbs = {
            TP1.partition: Mock(name='db1'),
            TP2.partition: Mock(name='db2'),
            TP3.partition: Mock(name='db3'),
            TP4.partition: Mock(name='db4'),
        }
        db_for_partition.side_effect = dbs.get

        store.set_persisted_offset = Mock(name='set_persisted_offset')

        store.apply_changelog_batch(events, None, None)

        rocks.WriteBatch.return_value.delete.assert_called_once_with('k5')
        rocks.WriteBatch.return_value.put.assert_has_calls([
            call('k1', 'v1'),
            call('k2', 'v2'),
            call('k3', 'v3'),
            call('k4', 'v4'),
        ])

        for db in dbs.values():
            db.write.assert_called_once_with(rocks.WriteBatch())

        store.set_persisted_offset.assert_has_calls([
            call(TP1, 1001),
            call(TP2, 2002),
            call(TP3, 3003),
            call(TP4, 4005),
        ])

    @pytest.yield_fixture()
    def current_event(self):
        with patch('faust.stores.rocksdb.current_event') as current_event:
            yield current_event.return_value

    def test__set(self, *, store, db_for_partition, current_event):
        store._set(b'key', b'value')
        db_for_partition.assert_called_once_with(
            current_event.message.partition)
        assert store._key_index[b'key'] == current_event.message.partition
        db_for_partition.return_value.put.assert_called_once_with(
            b'key', b'value',
        )

    def test_db_for_partition(self, *, store):
        ofp = store._open_for_partition = Mock(name='open_for_partition')

        assert store._db_for_partition(1) is ofp.return_value
        assert store._dbs[1] is ofp.return_value

        assert store._db_for_partition(1) is ofp.return_value

        ofp.assert_called_once_with(1)

    def test_open_for_partition(self, *, store):
        store.options.open = Mock(name='options.open')
        assert store._open_for_partition(1) is store.options.open.return_value
        store.options.open.assert_called_once_with(
            store.partition_path(1))

    def test__get__missing(self, *, store):
        store._get_bucket_for_key = Mock(name='get_bucket_for_key')
        store._get_bucket_for_key.return_value = None
        assert store._get(b'key') is None

    def test__get(self, *, store):
        db = Mock(name='db')
        value = b'foo'
        store._get_bucket_for_key = Mock(name='get_bucket_for_key')
        store._get_bucket_for_key.return_value = (db, value)

        assert store._get(b'key') == value

    def test__get__dbvalue_is_None(self, *, store):
        db = Mock(name='db')
        store._get_bucket_for_key = Mock(name='get_bucket_for_key')
        store._get_bucket_for_key.return_value = (db, None)

        db.key_may_exist.return_value = [False]
        assert store._get(b'key') is None

        db.key_may_exist.return_value = [True]
        db.get.return_value = None
        assert store._get(b'key') is None

        db.get.return_value = b'bar'
        assert store._get(b'key') == b'bar'

    def test_get_bucket_for_key__is_in_index(self, *, store):
        store._key_index[b'key'] = 30
        db = store._dbs[30] = Mock(name='db-p30')

        db.key_may_exist.return_value = [False]
        assert store._get_bucket_for_key(b'key') is None

        db.key_may_exist.return_value = [True]
        db.get.return_value = None
        assert store._get_bucket_for_key(b'key') is None

        db.get.return_value = b'value'
        assert store._get_bucket_for_key(b'key') == (db, b'value')

    def test_get_bucket_for_key__no_dbs(self, *, store):
        assert store._get_bucket_for_key(b'key') is None

    def new_db(self, name, exists=False):
        db = Mock(name=name)
        db.key_may_exist.return_value = [exists]
        db.get.return_value = name
        return db

    def test_get_bucket_for_key__not_in_index(self, *, store):

        dbs = {
            1: self.new_db(name='db1'),
            2: self.new_db(name='db2'),
            3: self.new_db(name='db3', exists=True),
            4: self.new_db(name='db4', exists=True),
        }
        store._dbs.update(dbs)

        assert store._get_bucket_for_key(b'key') == (dbs[3], 'db3')

    def test__del(self, *, store):
        dbs = store._dbs_for_key = Mock(return_value=[
            Mock(name='db1'),
            Mock(name='db2'),
            Mock(name='db3'),
        ])
        store._del(b'key')
        for db in dbs.return_value:
            db.delete.assert_called_once_with(b'key')

    @pytest.mark.asyncio
    async def test_on_rebalance(self, *, store, table):
        store.revoke_partitions = Mock(name='revoke_partitions')
        store.assign_partitions = AsyncMock(name='assign_partitions')

        assigned = {TP1, TP2}
        revoked = {TP3}
        newly_assigned = {TP2}
        await store.on_rebalance(table, assigned, revoked, newly_assigned)

        store.revoke_partitions.assert_called_once_with(table, revoked)
        store.assign_partitions.assert_called_once_with(table, newly_assigned)

    def test_revoke_partitions(self, *, store, table):
        table.changelog_topic.topics = {TP1.topic, TP3.topic}
        store._dbs[TP3.partition] = Mock(name='db')

        with patch('gc.collect') as collect:
            store.revoke_partitions(table, {TP1, TP2, TP3, TP4})
            assert not store._dbs

            collect.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_assign_partitions(self, *, store, app, table):
        app.assignor.assigned_standbys = Mock(return_value={TP4})
        table.changelog_topic.topics = list({
            tp.topic for tp in (TP1, TP2, TP4)
        })

        store._try_open_db_for_partition = AsyncMock()
        await store.assign_partitions(table, {TP1, TP2, TP3, TP4})
        store._try_open_db_for_partition.assert_has_calls([
            call(TP2.partition),
            call.coro(TP2.partition),
            call(TP1.partition),
            call.coro(TP1.partition),
        ], any_order=True)

    @pytest.mark.asyncio
    async def test_assign_partitions__empty_assignment(
            self, *, store, app, table):
        app.assignor.assigned_standbys = Mock(return_value={TP4})
        table.changelog_topic.topics = list({
            tp.topic for tp in (TP1, TP2, TP4)
        })
        await store.assign_partitions(table, set())

    @pytest.mark.asyncio
    async def test_open_db_for_partition(self, *, store, db_for_partition):
        with patch('faust.stores.rocksdb.rocksdb.errors.RocksIOError',
                   KeyError):
            assert (await store._try_open_db_for_partition(3) is
                    db_for_partition.return_value)

    @pytest.mark.asyncio
    async def test_open_db_for_partition_max_retries(
            self, *, store, db_for_partition):
        store.sleep = AsyncMock(name='sleep')
        with patch('faust.stores.rocksdb.rocksdb.errors.RocksIOError',
                   KeyError):
            db_for_partition.side_effect = KeyError('lock already')
            with pytest.raises(KeyError):
                await store._try_open_db_for_partition(3)
        assert store.sleep.call_count == 4

    @pytest.mark.asyncio
    async def test_open_db_for_partition__raises_unexpected_error(
            self, *, store, db_for_partition):
        with patch('faust.stores.rocksdb.rocksdb.errors.RocksIOError',
                   KeyError):
            db_for_partition.side_effect = RuntimeError()
            with pytest.raises(RuntimeError):
                await store._try_open_db_for_partition(3)

    @pytest.mark.asyncio
    async def test_open_db_for_partition_retries_recovers(
            self, *, store, db_for_partition):
        with patch('faust.stores.rocksdb.rocksdb.errors.RocksIOError',
                   KeyError):

            def on_call(partition):
                if db_for_partition.call_count < 3:
                    raise KeyError('lock already')
            db_for_partition.side_effect = on_call

            await store._try_open_db_for_partition(3)

    def test__contains(self, *, store):
        db1 = self.new_db('db1', exists=False)
        db2 = self.new_db('db2', exists=True)
        dbs = {b'key': [db1, db2]}
        store._dbs_for_key = Mock(side_effect=dbs.get)

        db2.get.return_value = None
        assert not store._contains(b'key')

        db2.get.return_value = b'value'
        assert store._contains(b'key')

    def test__dbs_for_key(self, *, store):
        dbs = store._dbs = {
            1: self.new_db('db1'),
            2: self.new_db('db2'),
            3: self.new_db('db3'),
        }
        store._key_index[b'key'] = 2

        assert list(store._dbs_for_key(b'other')) == list(dbs.values())
        assert list(store._dbs_for_key(b'key')) == [dbs[2]]

    def test__dbs_for_actives(self, *, store, table):
        table._changelog_topic_name.return_value = 'clog'
        store.app.assignor.assigned_actives = Mock(return_value=[
            TP('clog', 1),
            TP('clog', 2),
        ])
        dbs = store._dbs = {
            1: self.new_db('db1'),
            2: self.new_db('db2'),
            3: self.new_db('db3'),
        }

        assert list(store._dbs_for_actives()) == [dbs[1], dbs[2]]

    def test__size(self, *, store):
        dbs = self._setup_keys(
            db1=[
                store.offset_key,
                b'foo',
                b'bar',
            ],
            db2=[
                b'baz',
                store.offset_key,
                b'xuz',
                b'xaz',
            ],
        )
        store._dbs_for_actives = Mock(return_value=dbs)
        assert store._size() == 5

    def test__iterkeys(self, *, store):
        dbs = self._setup_keys(
            db1=[
                store.offset_key,
                b'foo',
                b'bar',
            ],
            db2=[
                b'baz',
                store.offset_key,
                b'xuz',
            ],
        )
        store._dbs_for_actives = Mock(return_value=dbs)

        assert list(store._iterkeys()) == [
            b'foo',
            b'bar',
            b'baz',
            b'xuz',
        ]

        for db in dbs:
            db.iterkeys.assert_called_once_with()
            db.iterkeys().seek_to_first.assert_called_once_with()

    def _setup_keys(self, **dbs: Mapping[str, List[bytes]]):
        return [
            self._setup_keys_db(name, values)
            for name, values in dbs.items()
        ]

    def _setup_keys_db(self, name: str, values: List[bytes]):
        db = self.new_db(name)
        db.iterkeys.return_value = MockIterator.from_values(values)
        return db

    def test__itervalues(self, *, store):
        dbs = self._setup_items(
            db1=[
                (store.offset_key, b'1001'),
                (b'k1', b'foo'),
                (b'k2', b'bar'),
            ],
            db2=[
                (b'k3', b'baz'),
                (store.offset_key, b'2002'),
                (b'k4', b'xuz'),
            ],
        )
        store._dbs_for_actives = Mock(return_value=dbs)

        assert list(store._itervalues()) == [
            b'foo',
            b'bar',
            b'baz',
            b'xuz',
        ]

        for db in dbs:
            db.iteritems.assert_called_once_with()
            db.iteritems().seek_to_first.assert_called_once_with()

    def _setup_items(self, **dbs: Mapping[str, List[Tuple[bytes, bytes]]]):
        return [
            self._setup_items_db(name, values)
            for name, values in dbs.items()
        ]

    def _setup_items_db(self, name: str, values: List[Tuple[bytes, bytes]]):
        db = self.new_db(name)
        db.iteritems.return_value = MockIterator.from_values(values)
        return db

    def test__iteritems(self, *, store):
        dbs = self._setup_items(
            db1=[
                (store.offset_key, b'1001'),
                (b'k1', b'foo'),
                (b'k2', b'bar'),
            ],
            db2=[
                (b'k3', b'baz'),
                (store.offset_key, b'2002'),
                (b'k4', b'xuz'),
            ],
        )
        store._dbs_for_actives = Mock(return_value=dbs)

        assert list(store._iteritems()) == [
            (b'k1', b'foo'),
            (b'k2', b'bar'),
            (b'k3', b'baz'),
            (b'k4', b'xuz'),
        ]

        for db in dbs:
            db.iteritems.assert_called_once_with()
            db.iteritems().seek_to_first.assert_called_once_with()

    def test_clear(self, *, store):
        with pytest.raises(NotImplementedError):
            store._clear()

    def test_reset_state(self, *, store):
        with patch('shutil.rmtree') as rmtree:
            store.reset_state()
            rmtree.assert_called_once_with(store.path.absolute())
