import os
import pytest
from case import Mock
from faust.utils.imports import symbol_by_name


@pytest.fixture()
def imp():
    return Mock(name='imp')


def test_when_ValueError(imp):
    imp.side_effect = ValueError
    with pytest.raises(ValueError):
        symbol_by_name('foo.bar:Baz', imp=imp)


@pytest.mark.parametrize('exc', [AttributeError, ImportError])
def test_when_ImportError(exc, imp):
    imp.side_effect = exc()
    with pytest.raises(exc):
        symbol_by_name('foo.bar:Baz', imp=imp, default=None)


@pytest.mark.parametrize('exc', [AttributeError, ImportError])
def test_when_ImportError__with_default(exc, imp):
    imp.side_effect = exc()
    assert symbol_by_name('foo.bar:Baz', imp=imp, default='f') == 'f'


def test_module():
    assert symbol_by_name('os') is os
