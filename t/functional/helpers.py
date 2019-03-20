import asyncio
from time import time
from faust.types.tuples import Message
from mode.utils.aiter import anext
import pytest

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


def message(key=None, value=None,
            *,
            topic='topic',
            partition=0,
            timestamp=None,
            headers=None,
            offset=1,
            checksum=None):
    return Message(
        key=key,
        value=value,
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp or time(),
        timestamp_type=1 if timestamp else 0,
        headers=headers,
        checksum=checksum,
    )


async def put(channel, key=None, value=None, **kwargs):
    msg = message(key=key, value=value, **kwargs)
    await channel.deliver(msg)
    return msg
