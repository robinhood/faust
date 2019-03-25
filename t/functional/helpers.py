import asyncio
from mode.utils.aiter import anext
import pytest
from t.helpers import message

__all__ = ['channel_empty', 'times_out', 'is_empty', 'message', 'put']


async def channel_empty(channel, *, timeout=0.01):
    assert channel.empty()
    with pytest.raises(asyncio.TimeoutError):
        await channel.get(timeout=timeout)
    return True


async def times_out(coro, *, timeout=0.01):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(coro, timeout=timeout)
    return True


async def is_empty(it, *, timeout=0.01):
    return await times_out(anext(it), timeout=timeout)


async def put(channel, key=None, value=None, **kwargs):
    msg = message(key=key, value=value, **kwargs)
    await channel.deliver(msg)
    return msg
