from typing import Mapping
from mode import setup_logging
import pytest
from .app import simple


setup_logging(loglevel='INFO')


def _build_data(i: int) -> Mapping:
    return {'A': {'the': {'quick': {'brown': {'fox': i}}}}}


@pytest.mark.asyncio
async def test_simple_ask() -> None:
    for i in range(100):
        value = _build_data(i)
        assert await simple.ask(value=value) == value


@pytest.mark.asyncio
async def test_simple_map() -> None:
    values = [_build_data(i) for i in range(100)]
    check = set(range(100))
    replies = set()
    async for reply in simple.map(values):
        v = reply['A']['the']['quick']['brown']['fox']
        assert v in check
        replies.add(v)
    assert replies == check
