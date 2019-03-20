import pytest
from faust import Event, Table
from faust.stores.base import SerializedStore, Store
from faust.types import TP
from mode import label
from mode.utils.mocks import Mock


class MyStore(Store):

    def __getitem__(self, key):
        ...

    def __setitem__(self, key, value):
        ...

    def __delitem__(self, key):
        ...

    def __iter__(self):
        ...

    def __len__(self):
        ...

    def apply_changelog_batch(self, *args, **kwargs):
        ...

    def reset_state(self):
        ...


class test_Store:

    @pytest.fixture
    def store(self, *, app):
        return MyStore(
            url='foo://',
            app=app,
            table=Mock(name='table'),
            key_serializer='json',
            value_serializer='json')

    def test_persisted_offset(self, *, store):
        with pytest.raises(NotImplementedError):
            store.persisted_offset(TP('foo', 0))

    def test_set_persisted_offset(self, *, store):
        store.set_persisted_offset(TP('foo', 0), 30303)

    @pytest.mark.asyncio
    async def test_need_active_standby_for(self, *, store):
        assert await store.need_active_standby_for(TP('foo', 0))

    @pytest.mark.asyncio
    async def test_on_rebalance(self, *, store):
        await store.on_rebalance(
            Mock(name='table', autospec=Table),
            set(),
            set(),
            set(),
        )

    def test_encode_key(self, *, store):
        assert store._encode_key({'foo': 1}) == b'{"foo": 1}'

    def test_encode_key__cannot_be_None(self, *, store):
        store.key_serializer = 'raw'
        with pytest.raises(TypeError):
            store._encode_key(None)

    def test_encode_value(self, *, store):
        assert store._encode_value({'foo': 1}) == b'{"foo": 1}'

    def test_decode_key(self, *, store):
        assert store._decode_key(b'{"foo": 1}') == {'foo': 1}

    def test_decode_value(self, *, store):
        assert store._decode_value(b'{"foo": 1}') == {'foo': 1}

    def test_repr(self, *, store):
        assert repr(store)

    def test_label(self, *, store):
        assert label(store)


class MySerializedStore(SerializedStore):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.keep = {}

    def _get(self, key):
        return self.keep.get(key)

    def _set(self, key, value):
        self.keep[key] = value

    def _del(self, key):
        self.keep.pop(key, None)

    def _iterkeys(self):
        return (k for k in self.keep)

    def _itervalues(self):
        return (self.keep[k] for k in self.keep)

    def _iteritems(self):
        return ((k, self.keep[k]) for k in self.keep)

    def _size(self):
        return len(self.keep)

    def _contains(self, key):
        return key in self.keep

    def _clear(self):
        self.keep.clear()

    def reset_state(self):
        ...


class test_SerializedStore:

    @pytest.fixture
    def store(self, *, app):
        return MySerializedStore(
            url='foo://',
            app=app,
            table=Mock(name='table'),
            key_serializer='json',
            value_serializer='json',
        )

    def test_apply_changelog_batch(self, *, store):
        event = Mock(name='event', autospec=Event)
        event.message.key = b'foo'
        event.message.value = b'bar'
        store.apply_changelog_batch([event], to_key=Mock(), to_value=Mock())
        assert store.keep[b'foo'] == b'bar'

    def test_apply_changelog_batch__delete_None_value(self, *, store):
        self.test_apply_changelog_batch(store=store)
        assert store.keep[b'foo'] == b'bar'
        event = Mock(name='event', autospec=Event)
        event.message.key = b'foo'
        event.message.value = None
        store.apply_changelog_batch([event], to_key=Mock(), to_value=Mock())
        with pytest.raises(KeyError):
            store.keep[b'foo']

    def test_apply_changelog_batch__key_is_None(self, *, store):
        event = Mock(name='event', autospec=Event)
        event.message.key = None
        event.message.value = b'bar'
        with pytest.raises(TypeError):
            store.apply_changelog_batch(
                [event], to_key=Mock(), to_value=Mock())

    def test_setitem__getitem__delitem(self, *, store):
        store['foo'] = '303'
        with pytest.raises(KeyError):
            store[{'foo': 2}]
        assert store['foo'] == '303'
        assert len(store) == 1
        assert 'foo' in store
        assert list(iter(store)) == ['foo']
        keys = store.keys()
        assert list(iter(keys)) == ['foo']
        values = store.values()
        assert list(iter(values)) == ['303']
        items = store.items()
        assert list(iter(items)) == [('foo', '303')]
        del(store['foo'])
        assert not len(store)
        store['foo'] = '303'
        store.clear()
        assert not len(store)
