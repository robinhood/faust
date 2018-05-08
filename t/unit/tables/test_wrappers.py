import operator
from datetime import datetime

import faust
import pytest
from faust.events import Event
from faust.exceptions import ImproperlyConfigured
from faust.tables.wrappers import WindowSet
from faust.types import Message
from mode.utils.mocks import Mock

DATETIME = datetime.utcnow()
DATETIME_TS = DATETIME.timestamp()


class User(faust.Record):
    id: str
    name: str


@pytest.fixture
def table(*, app):
    return app.Table('name')


@pytest.fixture
def wtable(*, table):
    return table.hopping(60, 1, 3600.0)


@pytest.fixture
def event():
    return Mock(name='event', autospec=Event)


class test_WindowSet:

    @pytest.fixture
    def wset(self, *, wtable, event):
        return WindowSet('k', wtable.table, wtable, event)

    def test_constructor(self, *, event, table, wset, wtable):
        assert wset.key == 'k'
        assert wset.table == table
        assert wset.wrapper == wtable
        assert wset.event == event

    def test_apply(self, *, wset, event):
        Mock(name='event2', autospec=Event)
        wset.wrapper.get_timestamp = Mock(name='wrapper.get_timestamp')
        wset.table._apply_window_op = Mock(name='_apply_window_op')

        ret = wset.apply(operator.add, 'val')
        wset.wrapper.get_timestamp.assert_called_once_with(wset.event)
        wset.table._apply_window_op.assert_called_once_with(
            operator.add, 'k', 'val', wset.wrapper.get_timestamp(),
        )
        assert ret is wset

    def mock_get_timestamp(self, wset):
        m = wset.wrapper.get_timestamp = Mock(name='wrapper.get_timestamp')
        return m

    def test_apply__custom_event(self, *, wset, event):
        event2 = Mock(name='event2', autospec=Event)
        wset.table._apply_window_op = Mock(name='_apply_window_op')
        get_timestamp = self.mock_get_timestamp(wset)

        ret = wset.apply(operator.add, 'val', event2)
        get_timestamp.assert_called_once_with(event2)
        wset.table._apply_window_op.assert_called_once_with(
            operator.add, 'k', 'val', get_timestamp(),
        )
        assert ret is wset

    def test_value(self, *, event, wset):
        get_timestamp = self.mock_get_timestamp(wset)
        wset.table._windowed_timestamp = Mock(name='_windowed_timestamp')
        assert wset.value(event)
        wset.table._windowed_timestamp.assert_called_once_with(
            'k', get_timestamp())

    def test_now(self, *, wset):
        wset.table._windowed_now = Mock(name='_windowed_now')
        ret = wset.now()
        wset.table._windowed_now.assert_called_once_with('k')
        assert ret is wset.table._windowed_now()

    def test_current(self, *, table, wset):
        event2 = Mock(name='event2', autospec=Event)
        table._windowed_timestamp = Mock(name='_windowed_timestamp')
        table._relative_event = Mock(name='_relative_event')
        ret = wset.current(event2)
        table._relative_event.assert_called_once_with(event2)
        table._windowed_timestamp.assert_called_once_with(
            'k', table._relative_event())
        assert ret is table._windowed_timestamp()

    def test_current__default_event(self, *, table, wset):
        table._windowed_timestamp = Mock(name='_windowed_timestamp')
        table._relative_event = Mock(name='_relative_event')
        ret = wset.current()
        table._relative_event.assert_called_once_with(wset.event)
        table._windowed_timestamp.assert_called_once_with(
            'k', table._relative_event())
        assert ret is table._windowed_timestamp()

    def test_delta(self, *, table, wset):
        event2 = Mock(name='event2', autospec=Event)
        table._windowed_delta = Mock(name='_windowed_delta')
        ret = wset.delta(30.3, event2)
        table._windowed_delta.assert_called_once_with('k', 30.3, event2)
        assert ret is table._windowed_delta()

    def test_delta__default_event(self, *, table, wset):
        table._windowed_delta = Mock(name='_windowed_delta')
        ret = wset.delta(30.3)
        table._windowed_delta.assert_called_once_with('k', 30.3, wset.event)
        assert ret is table._windowed_delta()

    def test_getitem(self, *, wset):
        wset.table = {(wset.key, 30.3): 101.1}
        assert wset[30.3] == 101.1

    def test_getitem__event(self, *, app, wset):
        e = Event(app,
                  key='KK',
                  value='VV',
                  message=Mock(name='message', autospec=Message))
        ret = wset[e]
        assert isinstance(ret, WindowSet)
        assert ret.key == wset.key
        assert ret.table is wset.table
        assert ret.wrapper is wset.wrapper
        assert ret.event is e

    def test_setitem(self, *, wset):
        wset.table = {}
        wset[30.3] = 'val'
        assert wset.table[(wset.key, 30.3)] == 'val'

    def test_setitem__event(self, *, app, wset):
        e = Event(app,
                  key='KK',
                  value='VV',
                  message=Mock(name='message', autospec=Message))
        with pytest.raises(NotImplementedError):
            wset[e] = 'val'

    def test_delitem(self, *, wset):
        wset.table = {(wset.key, 30.3): 'val'}
        del(wset[30.3])
        assert not wset.table

    def test_delitem__event(self, *, app, wset):
        e = Event(app,
                  key='KK',
                  value='VV',
                  message=Mock(name='message', autospec=Message))
        with pytest.raises(NotImplementedError):
            del(wset[e])

    @pytest.mark.parametrize('meth,expected_op', [
        ('__iadd__', operator.add),
        ('__isub__', operator.sub),
        ('__imul__', operator.mul),
        ('__itruediv__', operator.truediv),
        ('__ifloordiv__', operator.floordiv),
        ('__imod__', operator.mod),
        ('__ipow__', operator.pow),
        ('__ilshift__', operator.lshift),
        ('__irshift__', operator.rshift),
        ('__iand__', operator.and_),
        ('__ixor__', operator.xor),
        ('__ior__', operator.or_),
    ])
    def test_operators(self, meth, expected_op, *, wset):
        other = Mock(name='other')
        op = getattr(wset, meth)
        wset.apply = Mock(name='apply')
        result = op(other)

        wset.apply.assert_called_once_with(expected_op, other)
        assert result is wset.apply()

    def test_repr(self, *, wset):
        assert repr(wset)


class test_WindowWrapper:

    def test_name(self, *, wtable):
        assert wtable.name == wtable.table.name

    def test_relative_to(self, *, wtable):
        relative_to = Mock(name='relative_to')
        w2 = wtable.relative_to(relative_to)
        assert w2.table is wtable.table
        assert w2._get_relative_timestamp is relative_to

    def test_relative_to_now(self, *, table, wtable):
        w2 = wtable.relative_to_now()
        assert w2._get_relative_timestamp == wtable.table._relative_now

    def test_relative_to_field(self, *, table, wtable):
        table._relative_field = Mock(name='_relative_field')
        field = Mock(name='field')
        w2 = wtable.relative_to_field(field)
        table._relative_field.assert_called_once_with(field)
        assert w2._get_relative_timestamp == table._relative_field()

    def test_relative_to_stream(self, *, table, wtable):
        w2 = wtable.relative_to_stream()
        assert w2._get_relative_timestamp == wtable.table._relative_event

    @pytest.mark.parametrize('input,expected', [
        (DATETIME, DATETIME_TS),
        (303.333, 303.333),
        (None, 99999.6),
    ])
    def test_get_timestamp(self, input, expected, *, event, wtable):
        event.message.timestamp = 99999.6
        if input is not None:
            wtable.get_relative_timestamp = lambda e=None: input
        else:
            wtable.get_relative_timestamp = None
        assert wtable.get_timestamp(event) == expected

    def test_on_recover(self, *, wtable, table):
        cb = Mock(name='callback')
        wtable.on_recover(cb)
        assert cb in table._recover_callbacks

    def test_contains(self, *, table, wtable):
        table._windowed_contains = Mock(name='windowed_contains')
        wtable.get_timestamp = Mock(name='get_timestamp')
        ret = wtable.__contains__('k')
        wtable.get_timestamp.assert_called_once_with()
        table._windowed_contains.assert_called_once_with(
            'k', wtable.get_timestamp())
        assert ret is table._windowed_contains()

    def test_getitem(self, *, wtable):
        w = wtable['k2']
        assert isinstance(w, WindowSet)
        assert w.key == 'k2'
        assert w.table is wtable.table
        assert w.wrapper is wtable

    def test_setitem(self, *, table, wtable):
        table._set_windowed = Mock(name='set_windowed')
        wtable.get_timestamp = Mock(name='get_timestamp')
        wtable['foo'] = 300
        wtable.get_timestamp.assert_called_once_with()
        table._set_windowed.assert_called_once_with(
            'foo', 300, wtable.get_timestamp(),
        )

    def test_setitem__key_is_WindowSet(self, *, wtable):
        wtable['k2'] = wtable['k2']

    def test_delitem(self, *, table, wtable):
        table._del_windowed = Mock(name='del_windowed')
        wtable.get_timestamp = Mock(name='get_timestamp')
        del(wtable['foo'])
        wtable.get_timestamp.assert_called_once_with()
        table._del_windowed.assert_called_once_with(
            'foo', wtable.get_timestamp(),
        )

    def test_len(self, *, wtable):
        wtable.table = {1: 'A', 2: 'B'}
        assert len(wtable) == 2

    @pytest.mark.parametrize('input', [
        datetime.now(),
        103.33,
        User.id,
        lambda s: s,
    ])
    def test_relative_handler(self, input, *, wtable):
        wtable.get_relative_timestamp = input
        assert wtable.get_relative_timestamp

    def test_relative_handler__invalid_handler(self, *, wtable):
        with pytest.raises(ImproperlyConfigured):
            wtable._relative_handler(object())
