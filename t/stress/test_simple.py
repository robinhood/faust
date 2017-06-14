import asyncio
import pytest
from hypothesis import given
from hypothesis.strategies import (
    dictionaries, one_of, integers, booleans,
    floats, binary, text,
)
from .app import app, simple


@pytest.mark.asyncio
async def test_simple():
    data = dictionaries(text(),
                    one_of(integers(),
                           booleans(),
                           floats(),
                           text()))
    print('RUNNING TEST')
    value = data.example()
    print('STARTING: %r' % (value,))
    reply = await simple.ask(value=data.example())
    assert reply == value
    print('ENDED')

