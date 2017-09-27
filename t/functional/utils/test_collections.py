from case import Mock, call
from faust.utils.collections import FastUserDict, ManagedUserDict
import pytest


class test_FastUserDict:

    @pytest.fixture()
    def d(self):
        class X(FastUserDict):
            def __init__(self):
                self.data = {}
        return X()

    def test_setgetdel(self, d):
        with pytest.raises(KeyError):
            d['foo']
        d['foo'] = 303
        assert d['foo'] == 303
        d['foo'] = 606
        assert d['foo'] == 606
        del(d['foo'])
        with pytest.raises(KeyError):
            d['foo']

    def test_missing(self):
        m = Mock()

        class X(FastUserDict):

            def __init__(self):
                self.data = {}

            def __missing__(self, key):
                return m(key)

        x = X()
        assert x['foo'] is m.return_value
        assert x['foo'] is m.return_value
        assert m.call_count == 2

    def test_get(self, d):
        sentinel = object()
        assert d.get('foo', sentinel) is sentinel
        d['foo'] = 303
        assert d.get('foo') == 303

    def test_len(self, d):
        assert not d
        d['foo'] = 1
        assert len(d) == 1

    def test_iter(self, d):
        d.update(a=1, b=2, c=3)
        assert list(iter(d)) == ['a', 'b', 'c']

    def test_contains(self, d):
        assert 'foo' not in d
        d['foo'] = 1
        assert 'foo' in d

    def test_clear(self, d):
        d.update(a=1, b=2, c=3)
        assert d['a'] == 1
        assert d['b'] == 2
        assert d['c'] == 3
        assert len(d) == 3
        d.clear()
        assert not d
        for k in 'a', 'b', 'c':
            with pytest.raises(KeyError):
                d[k]

    def test_keys_items_values(self, d):
        src = {'a': 1, 'b': 2, 'c': 3}
        d.update(src)
        assert list(d.keys()) == list(src.keys())
        assert list(d.items()) == list(src.items())
        assert list(d.values()) == list(src.values())


class test_ManagedUserDict:

    @pytest.fixture
    def d(self):
        class X(ManagedUserDict):

            def __init__(self):
                self.key_get = Mock()
                self.key_set = Mock()
                self.key_del = Mock()
                self.cleared = Mock()
                self.data = {}

            def on_key_get(self, key):
                self.key_get(key)

            def on_key_set(self, key, value):
                self.key_set(key, value)

            def on_key_del(self, key):
                self.key_del(key)

            def on_clear(self):
                self.cleared()

        return X()

    def test_get_set_del(self, d):
        with pytest.raises(KeyError):
            d['foo']
        d.key_get.assert_called_once_with('foo')
        d['foo'] = 303
        d.key_set.assert_called_once_with('foo', 303)
        assert d['foo'] == 303
        assert d.key_get.call_count == 2

        del d['foo']
        d.key_del.assert_called_once_with('foo')
        with pytest.raises(KeyError):
            d['foo']
        assert d.key_get.call_count == 3

    def test_update__args(self, d):
        d.update({'a': 1, 'b': 2, 'c': 3})
        d.key_set.assert_has_calls([
            call('a', 1),
            call('b', 2),
            call('c', 3),
        ])

    def test_update__kwargs(self, d):
        d.update(a=1, b=2, c=3)
        d.key_set.assert_has_calls([
            call('a', 1),
            call('b', 2),
            call('c', 3),
        ])

    def test_cleaer(self, d):
        d.update(a=1, b=2, c=3)
        assert len(d) == 3
        d.clear()
        assert not d
        d.cleared.assert_called_once_with()
