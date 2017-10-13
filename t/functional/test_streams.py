import asyncio
from faust.utils.aiter import aiter, anext
import pytest
from .helpers import channel_empty, message, put


def new_stream(app):
    loop = asyncio.get_event_loop()
    app.loop = loop
    channel = app.channel(loop=loop, maxsize=1000)
    stream = channel.stream(loop=loop)
    return stream


@pytest.mark.asyncio
async def test_simple(app):
    stream = new_stream(app)
    stream_it = aiter(stream)
    assert await channel_empty(stream.channel)
    await put(stream.channel, key='key', value='value')
    assert await anext(stream_it) == 'value'
    assert await channel_empty(stream.channel)


@pytest.mark.asyncio
async def test_async_iterator(app):
    stream = new_stream(app)
    for i in range(100):
        await stream.channel.deliver(message(key=i, value=i))
    received = 0
    async for value in stream:
        assert value == received
        received += 1
        if received >= 100:
            break
    assert await channel_empty(stream.channel)


@pytest.mark.asyncio
async def test_throw(app):
    stream = new_stream(app)
    streamit = aiter(stream)
    await stream.channel.deliver(message(key='key', value='val'))
    assert await anext(streamit) == 'val'
    await stream.throw(KeyError('foo'))
    with pytest.raises(KeyError):
        await anext(streamit)


@pytest.mark.asyncio
async def test_enumerate(app):
    stream = new_stream(app)
    for i in range(100):
        await stream.channel.deliver(message(key=i, value=i * 4))
    async for i, value in stream.enumerate():
        current_event = stream.current_event
        assert i == current_event.key
        assert value == i * 4
        if i >= 99:
            break
    assert await channel_empty(stream.channel)


@pytest.mark.asyncio
async def test_items(app):
    stream = new_stream(app)
    for i in range(100):
        await stream.channel.deliver(message(key=i, value=i * 2))
    i = 0
    async for key, value in stream.items():
        assert key == i
        assert value == i * 2
        i += 1
        if i > 99:
            break
    assert await channel_empty(stream.channel)
