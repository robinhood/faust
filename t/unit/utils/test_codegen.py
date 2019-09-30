import pytest
from faust.utils.codegen import reprcall, reprkwargs


@pytest.mark.parametrize('input,expected', [
    ({}, ''),
    ({'foo': 'bar'}, "foo='bar'"),
    ({'foo': 'bar', 'xaz': 300.3}, "foo='bar', xaz=300.3"),
])
def test_reprkwargs(input, expected):
    assert reprkwargs(input) == expected


@pytest.mark.parametrize('name,args,kwargs,expected', [
    ('NAME', (), {}, 'NAME()'),
    ('NAME', (1, 2.3, 3), {}, 'NAME(1, 2.3, 3)'),
    ('NAME', (1, 2.3, 3), {'foo': 'bar'}, "NAME(1, 2.3, 3, foo='bar')"),
    ('NAME', (1, 2.3, 3), {'foo': 'bar', 'xaz': 300.3},
     "NAME(1, 2.3, 3, foo='bar', xaz=300.3)"),
])
def test_reprcall(name, args, kwargs, expected):
    assert reprcall(name, args, kwargs) == expected
