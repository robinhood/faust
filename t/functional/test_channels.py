import asyncio
import faust
from faust.types import StreamT, TP
from mode import label
from mode.utils.aiter import aiter, anext
from mode.utils.queues import FlowControlQueue
import pytest
from .helpers import channel_empty, message, times_out


class Point(faust.Record):
    x: int
    y: int


@pytest.fixture
def channel(*, app):
    return app.channel()


def test_repr(*, channel):
    assert repr(channel)


def test_repr__active_partitions_empty(*, channel):
    channel.active_partitions = set()
    assert repr(channel)


def test_repr__with_active_partitions(*, channel):
    channel.active_partitions = {TP('foo', 0), TP('foo', 1)}
    assert repr(channel)


def test_label(*, channel):
    assert label(channel)


def test_str(*, channel):
    assert str(channel)


def test_stream(*, channel):
    s = channel.stream()
    assert isinstance(s, StreamT)
    assert s.channel.queue is not channel.queue
    assert s.channel._root is channel
    assert s.channel in channel._subscribers


def test_get_topic_name(*, channel):
    with pytest.raises(NotImplementedError):
        channel.get_topic_name()


def test_clone(*, app):
    c = app.channel(key_type=Point, value_type=Point, maxsize=99, loop=33)
    assert isinstance(c.queue, FlowControlQueue)
    assert c.key_type is Point
    assert c.value_type is Point
    assert c.maxsize == 99
    assert c.loop == 33
    assert not c.is_iterator

    c2 = c.clone()
    assert c2.key_type is Point
    assert c2.value_type is Point
    assert c2.maxsize == 99
    assert c2.loop == 33
    assert not c2.is_iterator
    assert c2.queue is not c.queue
    assert c2._root is c
    assert c2 not in c._subscribers
    assert c.subscriber_count == 0

    c3 = c2.clone(is_iterator=True)
    assert c3.key_type is Point
    assert c3.value_type is Point
    assert c3.maxsize == 99
    assert c3.loop == 33
    assert c3.is_iterator
    assert c3.queue is not c.queue
    assert c3._root is c
    assert c2._root is c
    assert c3 in c._subscribers
    assert c2 not in c._subscribers
    assert c.subscriber_count == 1


@pytest.mark.asyncio
async def test_send_receive(*, app):
    app.flow_control.resume()
    channel1 = app.channel(maxsize=10)
    channel2 = app.channel(maxsize=1)
    with pytest.raises(RuntimeError):
        # must call aiter
        await channel1.__anext__()
    it1 = aiter(channel1)
    it2 = aiter(channel2)
    await channel2.put(b'xuzzy')
    assert await times_out(channel2.put(b'bar'))  # maxsize=1
    assert it1.queue is not channel1.queue
    assert it1._root is channel1
    assert it1 in channel1._subscribers
    assert channel1.subscriber_count == 1
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
    assert await anext(it2) == b'xuzzy'
    assert await channel_empty(channel2)

    it1_2 = aiter(channel1)
    assert it1_2._root is channel1
    assert it1_2 in channel1._subscribers
    assert channel1.subscriber_count == 2

    await channel1.put(b'moo')

    assert await anext(it1) == b'moo'
    assert await anext(it1_2) == b'moo'


@pytest.mark.asyncio
async def test_on_key_decode_error(*, app):
    channel = app.channel()
    await channel.on_key_decode_error(KeyError('foo'), 'msg')
    with pytest.raises(KeyError):
        await channel.get()


@pytest.mark.asyncio
async def test_on_value_decode_error(*, app):
    channel = app.channel()
    await channel.on_value_decode_error(KeyError('foo'), 'msg')
    with pytest.raises(KeyError):
        await channel.get()


def test_derive(*, app):
    channel = app.channel(maxsize=1)
    assert channel.derive() is channel


@pytest.mark.asyncio
async def test_declare__does_nothing(*, app):
    await app.channel().declare()


def test_clone_using_queue(*, channel):
    queue = asyncio.Queue()
    chan2 = channel.clone_using_queue(queue)
    assert chan2.queue is queue
    assert chan2.is_iterator


@pytest.mark.asyncio
async def test_interface_maybe_declare(*, channel):
    await channel.maybe_declare()


@pytest.mark.asyncio
async def test_decode(*, channel):
    msg = message(b'key', b'value')
    event = await channel.decode(msg)
    assert event.message is msg


@pytest.mark.asyncio
async def test_deliver(*, channel, app):
    app.flow_control.resume()
    msg = message(b'key', b'value')
    queue = channel.queue
    await channel.deliver(msg)
    event = queue.get_nowait()
    assert event.message is msg
