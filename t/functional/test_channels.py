import asyncio
from faust.utils.aiter import aiter, anext
import pytest


async def times_out(coro, *, timeout=0.01):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(coro, timeout=timeout)
    return True


async def is_empty(it, *, timeout=0.01):
    return await times_out(anext(it), timeout=timeout)


@pytest.mark.asyncio
async def test_send_receive(app):
    channel1 = app.channel(maxsize=10)
    channel2 = app.channel(maxsize=1)
    await channel2.put(b'xuzzy')
    assert await times_out(channel2.put(b'bar'))  # maxsize=1
    with pytest.raises(RuntimeError):
        # must call aiter
        await channel1.__anext__()
    it1 = aiter(channel1)
    assert it1.queue is channel1.queue
    assert channel1.queue is not channel2.queue
    assert channel1.errors is it1.errors
    assert channel1.errors is not channel2.errors
    assert await is_empty(it1)
    await channel1.put(b'foo')
    assert await anext(it1) == b'foo'
    assert await is_empty(it1)
    for i in range(10):
        await channel1.put(i)
    assert await times_out(channel1.put(10))  # maxsize=10
    for i in range(10):
        assert await anext(it1) == i
    assert await is_empty(it1)
    it2 = aiter(channel2)
    assert await anext(it2) == b'xuzzy'
    assert await is_empty(it2)



