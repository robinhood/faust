import faust
import pytest
from faust.tables.sets import (
    ChangeloggedSet,
    ChangeloggedSetManager,
    OPERATION_ADD,
    OPERATION_DISCARD,
    OPERATION_UPDATE,
    SetAction,
    SetManagerOperation,
    SetTableManager,
    SetWindowSet,
)
from mode.utils.mocks import AsyncMock, Mock, call


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


class test_SetTableManager:

    @pytest.fixture()
    def stable(self, *, app):
        return app.SetTable('name', start_manager=True)

    @pytest.fixture()
    def man(self, *, stable):
        return SetTableManager(stable)

    def test_constructor_enabled(self, *, app, stable):
        stable.start_manager = True
        app.agent = Mock(name='agent')
        man = SetTableManager(stable)
        assert man.enabled
        app.agent.assert_called_once_with(
            channel=man.topic,
            name='faust.SetTable.manager',
        )

    def test_constructor_disabled(self, *, app, stable):
        stable.start_manager = False
        app.agent = Mock(name='agent')
        man = SetTableManager(stable)
        assert man
        assert not man.enabled
        app.agent.assert_not_called()

    @pytest.mark.asyncio
    async def test_add(self, *, man):
        man._send_operation = AsyncMock()
        await man.add('key', 'member')
        man._send_operation.coro.assert_called_once_with(
            SetAction.ADD, 'key', ['member'],
        )

    @pytest.mark.asyncio
    async def test_discard(self, *, man):
        man._send_operation = AsyncMock()
        await man.discard('key', 'member')
        man._send_operation.coro.assert_called_once_with(
            SetAction.DISCARD, 'key', ['member'],
        )

    @pytest.mark.asyncio
    async def test_clear(self, *, man):
        man._send_operation = AsyncMock()
        await man.clear('key')
        man._send_operation.coro.assert_called_once_with(
            SetAction.CLEAR, 'key', [],
        )

    @pytest.mark.asyncio
    async def test_difference_update(self, *, man):
        man._send_operation = AsyncMock()
        await man.difference_update('key', ['v1', 'v2'])
        man._send_operation.coro.assert_called_once_with(
            SetAction.DISCARD, 'key', ['v1', 'v2'],
        )

    @pytest.mark.asyncio
    async def test_intersection_update(self, *, man):
        man._send_operation = AsyncMock()
        await man.intersection_update('key', ['v1', 'v2'])
        man._send_operation.coro.assert_called_once_with(
            SetAction.INTERSECTION, 'key', ['v1', 'v2'],
        )

    @pytest.mark.asyncio
    async def test_symmetric_difference_update(self, *, man):
        man._send_operation = AsyncMock()
        await man.symmetric_difference_update('key', ['v1', 'v2'])
        man._send_operation.coro.assert_called_once_with(
            SetAction.SYMDIFF, 'key', ['v1', 'v2'],
        )

    def test__update(self, *, man):
        man.set_table = {'a': Mock(name='a'), 'b': Mock(name='b')}
        man._update('a', ['v1'])
        man.set_table['a'].update.assert_called_once_with(['v1'])

    def test__difference_update(self, *, man):
        man.set_table = {'a': Mock(name='a'), 'b': Mock(name='b')}
        man._difference_update('a', ['v1'])
        man.set_table['a'].difference_update.assert_called_once_with(['v1'])

    def test__clear(self, *, man):
        man.set_table = {'a': Mock(name='a'), 'b': Mock(name='b')}
        man._clear('a', [])
        man.set_table['a'].clear.assert_called_once_with()

    def test__intersection_update(self, *, man):
        man.set_table = {'a': Mock(name='a'), 'b': Mock(name='b')}
        man._intersection_update('a', ['v1', 'v2', 'v3'])
        man.set_table['a'].intersection_update.assert_called_once_with(
            ['v1', 'v2', 'v3'],
        )

    def test__symmetric_difference_update(self, *, man):
        man.set_table = {'a': Mock(name='a'), 'b': Mock(name='b')}
        man._symmetric_difference_update('a', ['v1', 'v2', 'v3'])
        man.set_table['a'].symmetric_difference_update.assert_called_once_with(
            ['v1', 'v2', 'v3'],
        )

    @pytest.mark.asyncio
    async def test__send_operation__disabled(self, *, man):
        man.enabled = False
        with pytest.raises(RuntimeError):
            await man._send_operation(SetAction.ADD, 'k', 'v')

    @pytest.mark.asyncio
    async def test__send_operation__enabled(self, *, man):
        man.enabled = True
        man.topic.send = AsyncMock()
        await man._send_operation(SetAction.ADD, 'k', ['v'])
        man.topic.send.assert_called_once_with(
            key='k',
            value=SetManagerOperation(action=SetAction.ADD, members=['v']),
        )

    @pytest.mark.asyncio
    async def test__send_operation__enabled__iterator(self, *, man):
        man.enabled = True
        man.topic.send = AsyncMock()
        await man._send_operation(SetAction.ADD, 'k', iter(['a', 'b']))
        man.topic.send.assert_called_once_with(
            key='k',
            value=SetManagerOperation(
                action=SetAction.ADD,
                members=['a', 'b'],
            ),
        )

    @pytest.mark.asyncio
    @pytest.mark.allow_lingering_tasks(count=1)
    async def test__modify_set(self, *, man):
        stream = Mock()
        man.set_table = {
            'k1': Mock(name='k1'),
            'k2': Mock(name='k2'),
            'k3': Mock(name='k3'),
            'k4': Mock(name='k4'),
            'k5': Mock(name='k5'),
            'k6': Mock(name='k6'),
            'k7': Mock(name='k7'),
            'k8': Mock(name='k8'),
        }

        class X(faust.Record):
            x: int
            y: int

        unknown_set_op = SetManagerOperation(
            action=SetAction.ADD,
            members=['v4'],
        )
        unknown_set_op.action = 'UNKNOWN'

        async def stream_items():
            yield ('k1', SetManagerOperation(
                action=SetAction.ADD,
                members=['v'],
            ))
            yield ('k2', SetManagerOperation(
                action=SetAction.DISCARD,
                members=['v2'],
            ))
            yield ('k3', SetManagerOperation(
                action=SetAction.DISCARD,
                members=[X(10, 30).to_representation()],
            ))
            yield ('k4', unknown_set_op)
            yield ('k5', SetManagerOperation(
                action=SetAction.ADD,
                members=[
                    X(10, 30).to_representation(),
                    X(20, 40).to_representation(),
                    'v3',
                ],
            ))
            yield ('k6', SetManagerOperation(
                action=SetAction.INTERSECTION,
                members=[
                    X(10, 30).to_representation(),
                    X(20, 40).to_representation(),
                    'v3',
                ],
            ))
            yield ('k7', SetManagerOperation(
                action=SetAction.SYMDIFF,
                members=[
                    X(10, 30).to_representation(),
                    X(20, 40).to_representation(),
                    'v3',
                ],
            ))
            yield ('k8', SetManagerOperation(
                action=SetAction.CLEAR,
                members=[],
            ))

        stream.items.side_effect = stream_items

        await man._modify_set(stream)

        man.set_table['k1'].update.assert_called_with(['v'])
        man.set_table['k2'].difference_update.assert_called_with(['v2'])
        man.set_table['k3'].difference_update.assert_called_with([X(10, 30)])
        man.set_table['k5'].update.assert_called_with([
            X(10, 30),
            X(20, 40),
            'v3',
        ])
        man.set_table['k6'].intersection_update.assert_called_with([
            X(10, 30),
            X(20, 40),
            'v3',
        ])
        man.set_table['k7'].symmetric_difference_update.assert_called_with([
            X(10, 30),
            X(20, 40),
            'v3',
        ])
        man.set_table['k8'].clear.assert_called_once_with()


class test_SetTable:

    @pytest.fixture()
    def stable(self, *, app):
        return app.SetTable('name')

    def test_constructor__with_suffix(self, *, app):
        t1 = app.SetTable('name', manager_topic_suffix='-foo')
        assert t1.manager_topic_suffix == '-foo'
        assert t1.manager_topic_name == 'name-foo'

    def test_constructor__with_specific_name(self, *, app):
        t1 = app.SetTable('name', manager_topic_name='foo')
        assert t1.manager_topic_name == 'foo'

    @pytest.mark.asyncio
    async def test_on_start(self, *, stable):
        stable.changelog_topic.maybe_declare = AsyncMock()
        stable.add_runtime_dependency = AsyncMock()
        stable.start_manager = False
        await stable.on_start()

        stable.start_manager = True
        await stable.on_start()
        stable.add_runtime_dependency.assert_has_calls([
            call(stable.manager)], any_order=True)
        await stable.on_stop()

    def test__new_store(self, *, stable):
        store = stable._new_store()
        assert isinstance(store, ChangeloggedSetManager)
        assert store.table is stable

    def test__getitem__(self, *, stable):
        assert stable['foo'] is not None
