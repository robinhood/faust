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
        event, to_key, to_value = self.mock_event_to_key_value()
        store.apply_changelog_batch([event], to_key=to_key, to_value=to_value)

        to_key.assert_called_once_with(b'key')
        to_value.assert_called_once_with(b'value')
        assert store.data[to_key()] == to_value()

    def test_apply_changelog_batch__deletes_key_for_None_value(self, *, store):
        self.test_apply_changelog_batch(store=store)
        event2, to_key, to_value = self.mock_event_to_key_value(value=None)

        assert to_key() in store.data
        store.apply_changelog_batch([event2], to_key=to_key, to_value=to_value)

        assert to_key() not in store.data

    def mock_event_to_key_value(self, key=b'key', value=b'value'):
        event = self.mock_event(key=key, value=value)
        to_key, to_value = self.mock_to_key_value(event)
        return event, to_key, to_value

    def mock_event(self, key=b'key', value=b'value'):
        event = Mock(name='event', autospec=Event)
        event.key = key
        event.value = value
        event.message.key = key
        event.message.value = value
        return event

    def mock_to_key_value(self, event):
        to_key = Mock(name='to_key')
        to_key.return_value = event.key
        to_value = Mock(name='to_value')
        to_value.return_value = event.value
        return to_key, to_value

    def test_persisted_offset(self, *, store):
        assert store.persisted_offset(TP('foo', 0)) is None

    def test_reset_state(self, *, store):
        store.reset_state()
