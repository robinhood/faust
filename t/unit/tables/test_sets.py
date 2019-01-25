import pytest
from faust.tables.sets import (
    ChangeloggedSet,
    ChangeloggedSetManager,
    OPERATION_ADD,
    OPERATION_DISCARD,
    OPERATION_UPDATE,
    SetWindowSet,
)
from mode.utils.mocks import Mock, call


@pytest.fixture()
def key():
    return Mock(name='key')


@pytest.fixture()
def table():
    return Mock(name='table')


class test_SetWindowSet:

    @pytest.fixture()
    def wrapper(self):
        return Mock(name='wrapper')

    @pytest.fixture()
    def wset(self, *, key, table, wrapper):
        return SetWindowSet(key, table, wrapper)

    def test_add(self, *, wset):
        event = Mock(name='event')
        wset._apply_set_operation = Mock()
        wset.add('value', event=event)
        wset._apply_set_operation.assert_called_once_with(
            'add', 'value', event,
        )

    def test_discard(self, *, wset):
        event = Mock(name='event')
        wset._apply_set_operation = Mock()
        wset.discard('value', event=event)
        wset._apply_set_operation.assert_called_once_with(
            'discard', 'value', event,
        )

    def test__apply_set_operation(self, *, wset, key, table, wrapper):
        event = Mock(name='event')
        wrange1 = Mock(name='window_range1')
        wrange2 = Mock(name='window_range2')
        table._window_ranges.return_value = [wrange1, wrange2]

        wset._apply_set_operation('op', 'value', event)

        wset.wrapper.get_timestamp.assert_called_once_with(event)
        wrapper.on_set_key.assert_called_once_with(key, 'value')
        table._get_key.assert_has_calls([
            call((key, wrange1)),
            call().op('value'),
            call((key, wrange2)),
            call().op('value'),
        ])


class test_ChangeloggedSet:

    @pytest.fixture()
    def manager(self):
        return Mock(name='manager')

    @pytest.fixture()
    def cset(self, *, manager, key):
        return ChangeloggedSet(manager, key)

    def test_constructor(self, *, cset):
        assert isinstance(cset.data, set)

    def test_on_add(self, *, cset, manager, key):
        cset.add('value')
        manager.send_changelog_event.assert_called_once_with(
            key, OPERATION_ADD, 'value',
        )

    def test_on_discard(self, *, cset, manager, key):
        cset.data.add('value')
        cset.discard('value')
        manager.send_changelog_event.assert_called_once_with(
            key, OPERATION_DISCARD, 'value',
        )

    def test_on_change__diff(self, *, cset, manager, key):
        cset.data.update({1, 2, 3, 4})
        cset.difference_update({2, 3, 4, 5, 6})
        manager.send_changelog_event.assert_called_once_with(
            key, OPERATION_UPDATE, [set(), {2, 3, 4}],
        )

    def test_on_change__update(self, *, cset, manager, key):
        cset.data.update({1, 2, 3, 4})
        cset.update({2, 3, 4, 5, 6})
        manager.send_changelog_event.assert_called_once_with(
            key, OPERATION_UPDATE, [{5, 6}, set()],
        )

    def test_sync_from_storage(self, *, cset):
        cset.data.update({1, 2, 3})
        cset.sync_from_storage({2, 3, 4})
        assert cset.data == {2, 3, 4}

    def test_as_stored_value(self, *, cset):
        cset.data.update({1, 2, 3})
        assert cset.as_stored_value() == {1, 2, 3}

    def test_apply_changelog_event__ADD(self, *, cset):
        cset.data.update({2})
        cset.apply_changelog_event(OPERATION_ADD, 3)
        assert cset.data == {2, 3}

    def test_apply_changelog_event__DISCARD(self, *, cset):
        cset.data.update({2, 3})
        cset.apply_changelog_event(OPERATION_DISCARD, 2)
        assert cset.data == {3}

    def test_apply_changelog_event__UPDATE(self, *, cset):
        cset.data.update({5, 6, 7, 8, 9})
        added = {2, 3, 4}
        removed = {6, 7, 8}
        cset.apply_changelog_event(OPERATION_UPDATE, [added, removed])
        assert cset.data == {2, 3, 4, 5, 9}

    def test_apply_changelog_event__not_implemented(self, *, cset):
        with pytest.raises(NotImplementedError):
            cset.apply_changelog_event(0xffff, {1, 2, 3})


def test_ChangeloggedSetManager():
    assert ChangeloggedSetManager.ValueType is ChangeloggedSet


class test_SetTable:

    @pytest.fixture()
    def stable(self, *, app):
        return app.SetTable('name')

    def test__new_store(self, *, stable):
        store = stable._new_store()
        assert isinstance(store, ChangeloggedSetManager)
        assert store.table is stable

    def test__getitem__(self, *, stable):
        assert stable['foo'] is not None
