import pytest
from faust import Event
from faust.stores.memory import Store
from faust.types import TP
from mode.utils.mocks import Mock


class test_Store:

    @pytest.fixture
    def store(self, *, app):
        return Store(url='memory://', app=app)

    def test_clear(self, *, store):
        store.data['foo'] = 1
        store._clear()
        assert not store.data

    def test_apply_changelog_batch(self, *, store):
        event1 = Mock(name='event1', autospec=Event)
        event1.key = b'key'
        event1.value = b'value'
        events = [event1]
        to_key = Mock(name='to_key')
        to_value = Mock(name='to_value')
        store.apply_changelog_batch(events, to_key=to_key, to_value=to_value)

        to_key.assert_called_once_with(b'key')
        to_value.assert_called_once_with(b'value')
        assert store.data[to_key()] is to_value()

    def test_persisted_offset(self, *, store):
        assert store.persisted_offset(TP('foo', 0)) is None

    def test_reset_state(self, *, store):
        store.reset_state()
