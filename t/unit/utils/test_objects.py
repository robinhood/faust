import pytest
from faust.utils.objects import cached_property


class test_cached_property:

    class X:

        @cached_property
        def foo(self):
            return 42

    class X_setter:
        _foo = 1

        @cached_property
        def foo(self):
            return self._foo

        @foo.setter
        def foo(self, value):
            self._foo = value
            return value

    class X_deleter:
        _foo = 1

        @cached_property
        def foo(self):
            return self._foo

        @foo.deleter
        def foo(self, value):
            assert value == 1
            self._foo = None

    @pytest.fixture()
    def x(self):
        return self.X()

    @pytest.fixture()
    def x_setter(self):
        return self.X_setter()

    @pytest.fixture()
    def x_deleter(self):
        return self.X_deleter()

    def test_get(self, x):
        assert 'foo' not in x.__dict__
        assert x.foo == 42
        assert x.__dict__['foo'] == 42
        assert x.foo == 42

    def test_get_class(self, x):
        assert type(x).foo.__get__(None) is type(x).foo

    def test_get_setter(self, x_setter):
        assert x_setter.foo == 1

    def test_set(self, x):
        assert x.foo == 42
        x.foo = 303
        assert x.foo == 303
        assert x.__dict__['foo'] == 303

    def test_set_class(self, x):
        assert type(x).foo.__set__(None, 1) is type(x).foo

    def test_set_setter(self, x_setter):
        assert x_setter.foo == 1
        x_setter.foo = 2
        assert x_setter.foo == 2
        assert x_setter._foo == 2

    def test_del(self, x):
        assert 'foo' not in x.__dict__
        assert x.foo == 42
        assert 'foo' in x.__dict__
        del x.foo
        assert 'foo' not in x.__dict__

    def test_del_class(self, x):
        assert type(x).foo.__delete__(None) is type(x).foo

    def test_del_deleter(self, x_deleter):
        del x_deleter.foo
        assert x_deleter._foo == 1
        assert x_deleter.foo == 1
        del x_deleter.foo
        assert x_deleter._foo is None
