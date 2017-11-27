import faust
from faust.types import StreamT
from faust.utils.aiter import aiter, anext
from faust.utils.futures import FlowControlQueue
from mode import label
import pytest
from .helpers import channel_empty, times_out


class Point(faust.Record):
    x: int
    y: int


@pytest.fixture
def channel(app):
    return app.channel()


def test_repr(channel):
    assert repr(channel)


def test_label(channel):
    assert label(channel)


def test_str(channel):
    assert str(channel)


def test_stream(channel):
    s = channel.stream()
    assert isinstance(s, StreamT)
    assert s.channel.queue is channel.queue


def test_get_topic_name(channel):
    with pytest.raises(NotImplementedError):
        channel.get_topic_name()


def test_clone(app):
    c = app.channel(key_type=Point, value_type=Point, maxsize=99, loop=33)
    assert isinstance(c.queue, FlowControlQueue)
    assert c.key_type is Point
    assert c.value_type is Point
    assert c.maxsize == 99
    assert c.loop == 33
    assert c.clone_shares_queue
    assert not c.is_iterator

    c2 = c.clone()
    assert c2.key_type is Point
    assert c2.value_type is Point
    assert c2.maxsize == 99
    assert c2.loop == 33
    assert not c2.is_iterator
    assert c2.queue is c.queue

    c3 = c2.clone(is_iterator=True)
    assert c3.key_type is Point
    assert c3.value_type is Point
    assert c3.maxsize == 99
    assert c3.loop == 33
    assert c3.is_iterator
    assert c3.queue is c.queue


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
    assert await channel_empty(channel1)
    await channel1.put(b'foo')
    assert await anext(it1) == b'foo'
    assert await channel_empty(channel1)
    for i in range(10):
        await channel1.put(i)
    assert await times_out(channel1.put(10))  # maxsize=10
    for i in range(10):
        assert await anext(it1) == i
    assert await channel_empty(channel1)
    it2 = aiter(channel2)
    assert await anext(it2) == b'xuzzy'
    assert await channel_empty(channel2)
