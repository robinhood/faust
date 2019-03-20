import asyncio
from copy import copy

import pytest
from faust.exceptions import ImproperlyConfigured
from faust.streams import maybe_forward
from mode import label
from mode.utils.aiter import aiter, anext
from mode.utils.mocks import AsyncMock, Mock

from .helpers import channel_empty, message, put


def new_stream(app, *args, **kwargs):
    app = _prepare_app(app)
    return _new_stream(app, app.channel(loop=app.loop, maxsize=1000), **kwargs)


def new_topic_stream(app, *args, name: str = 'test', **kwargs):
    app = _prepare_app(app)
    return _new_stream(app, app.topic(name, loop=app.loop), **kwargs)


def _new_stream(app, channel, *args, **kwargs):
    return channel.stream(*args, loop=app.loop, **kwargs)


def _prepare_app(app):
    loop = asyncio.get_event_loop()
    app.loop = loop
    app.flow_control.resume()  # <-- flow control initially suspended
    return app


@pytest.mark.asyncio
@pytest.mark.allow_lingering_tasks(count=1)
async def test_simple(app, loop):
    async with new_stream(app) as stream:
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


@pytest.mark.asyncio
async def test_through(app):
    app._attachments.enabled = False
    orig = new_stream(app)
    channel = app.channel(loop=app.loop)
    stream = orig.through(channel)
    for i in range(100):
        await orig.channel.deliver(message(key=i, value=i * 2))

    assert stream.get_root_stream() is orig
    assert orig._passive
    # noop
    orig._enable_passive(orig.channel)

    events = []
    async for i, value in stream.enumerate():
        assert value == i * 2
        assert stream.current_event
        events.append(mock_event_ack(stream.current_event))
        if i >= 99:
            break
    await orig.stop()
    await stream.stop()
    assert_events_acked(events)


def test_through_with_concurrency_index(app):
    s = new_stream(app)
    s.concurrency_index = 0

    with pytest.raises(ImproperlyConfigured):
        s.through('foo')


@pytest.mark.asyncio
async def test_through_twice(app):
    async with new_topic_stream(app) as s:
        s._enable_passive = Mock()
        async with s.through('bar') as s2:
            s2._enable_passive = Mock()
            with pytest.raises(ImproperlyConfigured):
                s.through('baz')


def test_group_by_with_concurrency_index(app):
    s = new_stream(app)
    s.concurrency_index = 0

    with pytest.raises(ImproperlyConfigured):
        s.group_by(lambda s: s.foo)


def test_group_by_callback_must_have_name(app):
    s = new_topic_stream(app)
    with pytest.raises(TypeError):
        s.group_by(lambda s: s.foo)


@pytest.mark.asyncio
async def test_group_by_twice(app):
    async with new_topic_stream(app) as s:
        s._enable_passive = Mock()
        async with s.group_by(lambda s: s.foo, name='foo') as s2:
            s2._enable_passive = Mock()
            with pytest.raises(ImproperlyConfigured):
                s.group_by(lambda s: s.foo, name='foo')


@pytest.mark.asyncio
async def test_stream_over_iterable(app):
    async with app.stream([0, 1, 2, 3, 4, 5]) as s:
        i = 0
        async for value in s:
            assert value == i
            i += 1


@pytest.mark.asyncio
async def test_events(app):
    stream = new_stream(app)
    for i in range(100):
        await stream.channel.deliver(message(key=i, value=i * 2))
        await stream.send(i)  # no associated event
    i = 0
    events = []
    async for event in stream.events():
        assert event.key == i
        assert event.value == i * 2
        events.append(mock_event_ack(event))
        i += 1
        if i > 99:
            break
    await stream.stop()
    await asyncio.sleep(0)  # have to sleep twice here for all events to be
    await asyncio.sleep(0.5)  # acked for some reason
    assert_events_acked(events)


def assert_events_acked(events):
    try:
        for event in events:
            if not event.ack.called:
                assert event.message.acked
                assert not event.message.refcount
    except AssertionError:
        fail_count = len([e for e in events if not e.ack.call_count])
        fail_positions = [i for i, e in enumerate(events)
                          if not e.ack.call_count]
        print(f'ACK FAILED FOR {fail_count} EVENT(S)')
        print(f'  POSITIONS: {fail_positions}')
        raise


class test_chained_streams:

    def _chain(self, app):
        root = new_stream(app)
        root._next = s1 = new_stream(app, prev=root)
        s1._next = s2 = new_stream(app, prev=s1)
        s2._next = s3 = new_stream(app, prev=s2)
        return root, s1, s2, s3

    def test_get_root_stream(self, app):
        root, s1, s2, s3 = self._chain(app)
        assert root.get_root_stream() is root
        assert s3.get_root_stream() is root
        assert s2.get_root_stream() is root
        assert s1.get_root_stream() is root

    def test_get_active_stream(self, app):
        root, s1, s2, s3 = self._chain(app)
        assert root.get_active_stream() is s3
        assert s1.get_active_stream() is s3
        assert s2.get_active_stream() is s3
        assert s3.get_active_stream() is s3

    def test_iter_ll_forwards(self, app):
        root, s1, s2, s3 = self._chain(app)
        assert list(root._iter_ll_forwards()) == [root, s1, s2, s3]
        assert list(s1._iter_ll_forwards()) == [s1, s2, s3]
        assert list(s2._iter_ll_forwards()) == [s2, s3]
        assert list(s3._iter_ll_forwards()) == [s3]
        root_root = root.get_root_stream()
        assert root_root is root
        assert list(root_root._iter_ll_forwards()) == [root, s1, s2, s3]
        s1_root = s1.get_root_stream()
        assert s1_root is root
        assert list(s1_root._iter_ll_forwards()) == [root, s1, s2, s3]
        s2_root = s2.get_root_stream()
        assert s2_root is root
        assert list(s2_root._iter_ll_forwards()) == [root, s1, s2, s3]
        s3_root = s3.get_root_stream()
        assert s3_root is root
        assert list(s3_root._iter_ll_forwards()) == [root, s1, s2, s3]

    def test_iter_ll_backwards(self, app):
        root, s1, s2, s3 = self._chain(app)
        assert list(root._iter_ll_backwards()) == [root]
        assert list(s1._iter_ll_backwards()) == [s1, root]
        assert list(s2._iter_ll_backwards()) == [s2, s1, root]
        assert list(s3._iter_ll_backwards()) == [s3, s2, s1, root]
        root_active = root.get_active_stream()
        assert root_active is s3
        assert list(root_active._iter_ll_backwards()) == [s3, s2, s1, root]
        s1_active = s1.get_active_stream()
        assert s1_active is s3
        assert list(s1_active._iter_ll_backwards()) == [s3, s2, s1, root]
        s2_active = s2.get_active_stream()
        assert s2_active is s3
        assert list(s2_active._iter_ll_backwards()) == [s3, s2, s1, root]
        s3_active = s3.get_active_stream()
        assert s3_active is s3
        assert list(s3_active._iter_ll_backwards()) == [s3, s2, s1, root]

    def test_get_active_stream__loop_in_linkedlist(self, app):
        root, s1, s2, s3 = self._chain(app)
        s2._next = s1
        with pytest.raises(RuntimeError):
            root.get_active_stream()

    def test_get_root_stream__loop_in_linkedlist(self, app):
        root, s1, s2, s3 = self._chain(app)
        s1._prev = s2
        with pytest.raises(RuntimeError):
            s3.get_root_stream()

    @pytest.mark.asyncio
    async def test_stop_stops_chain__root(self, app):
        root, s1, s2, s3 = self._chain(app)
        await self.assert_was_stopped(root, [s1, s2, s3])

    @pytest.mark.asyncio
    async def test_stop_stops_chain__s1(self, app):
        root, s1, s2, s3 = self._chain(app)
        await self.assert_was_stopped(s1, [root, s2, s3])

    @pytest.mark.asyncio
    async def test_stop_stops_chain__s2(self, app):
        root, s1, s2, s3 = self._chain(app)
        await self.assert_was_stopped(s2, [root, s1, s3])

    @pytest.mark.asyncio
    async def test_stop_stops_chain__s3(self, app):
        root, s1, s2, s3 = self._chain(app)
        await self.assert_was_stopped(s3, [root, s1, s2])

    async def assert_was_stopped(self, leader, followers):
        all_nodes = [leader] + list(followers)
        for node in all_nodes:
            assert not node._stopped.set()
        for node in all_nodes:
            node._started.set()
        await leader.stop()
        for node in all_nodes:
            assert node._stopped.is_set()


@pytest.mark.asyncio
async def test_start_and_stop_Stream(app):
    s = new_topic_stream(app)
    await _start_stop_stream(s)
    await app.producer.stop()


async def _start_stop_stream(stream):
    assert not stream._passive
    await stream.start()
    stream.__aiter__()
    assert stream.app.topics
    await stream.stop()


@pytest.mark.asyncio
async def test_ack(app):
    s = new_stream(app)
    assert s.get_active_stream() is s
    await s.channel.send(value=1)
    await s.channel.send(value=2)
    event = None
    i = 1
    async for value in s:
        assert value == i
        i += 1
        last_to_ack = value == 2
        event = mock_stream_event_ack(s, return_value=last_to_ack)
        if value == 2:
            break
    assert event
    # need one sleep on Python 3.6.0-3.6.6 + 3.7.0
    # need two sleeps on Python 3.6.7 + 3.7.1 :-/
    await asyncio.sleep(0)  # needed for some reason
    await asyncio.sleep(0)  # needed for some reason
    if not event.ack.called:
        assert event.message.acked
        assert not event.message.refcount


@pytest.mark.asyncio
async def test_noack(app):
    s = new_stream(app)
    new_s = s.noack()
    assert new_s is not s
    assert s.get_active_stream() is new_s
    await new_s.channel.send(value=1)
    event = None
    async for value in new_s:
        assert value == 1
        event = mock_stream_event_ack(new_s)
        break
    assert event
    # need one sleep on Python 3.6.0-3.6.6 + 3.7.0
    # need two sleeps on Python 3.6.7 + 3.7.1 :-/
    await asyncio.sleep(0)  # needed for some reason
    await asyncio.sleep(0)  # needed for some reason
    event.ack.assert_not_called()


@pytest.mark.asyncio
async def test_acked_when_raising(app):
    s = new_stream(app)
    await s.channel.send(value=1)
    await s.channel.send(value=2)

    event1 = None
    with pytest.raises(RuntimeError):
        async for value in s:
            event1 = mock_stream_event_ack(s)
            assert value == 1
            raise RuntimeError
    assert event1
    # need one sleep on Python 3.6.0-3.6.6 + 3.7.0
    # need two sleeps on Python 3.6.7 + 3.7.1 :-/
    await asyncio.sleep(0)  # needed for some reason
    await asyncio.sleep(0)  # needed for some reason
    if not event1.ack.called:
        assert event1.message.acked
        assert not event1.message.refcount

    event2 = None
    with pytest.raises(RuntimeError):
        async for value in s:
            event2 = mock_stream_event_ack(s)
            assert value == 2
            raise RuntimeError
    assert event2
    # need one sleep on Python 3.6.0-3.6.6 + 3.7.0
    # need two sleeps on Python 3.6.7 + 3.7.1 :-/
    await asyncio.sleep(0)  # needed for some reason
    await asyncio.sleep(0)  # needed for some reason
    if not event2.ack.called:
        assert event2.message.acked
        assert not event2.message.refcount


@pytest.mark.asyncio
@pytest.mark.allow_lingering_tasks(count=1)
async def test_maybe_forward__when_event(app):
    async with new_stream(app) as s:
        event = await get_event_from_value(s, 'foo')
        s.channel.send = Mock(name='channel.send')
        event.forward = AsyncMock(name='event.forward')
        await maybe_forward(event, s.channel)
        event.forward.assert_called_once_with(s.channel)
        s.channel.send.assert_not_called()


@pytest.mark.asyncio
async def test_maybe_forward__when_concrete_value(app):
    s = new_stream(app)
    s.channel.send = AsyncMock(name='channel.send')
    await maybe_forward('foo', s.channel)
    s.channel.send.assert_called_once_with(value='foo')


def mock_stream_event_ack(stream, return_value=False):
    return mock_event_ack(stream.current_event, return_value=return_value)


def mock_event_ack(event, return_value=False):
    event.ack = Mock(name='ack')
    event.ack.return_value = return_value
    return event


async def get_event_from_value(stream, value, key=None):
    await stream.channel.send(key=key, value=value)
    async for value in stream:
        event = stream.current_event
        assert event
        event.ack = Mock(name='event.ack')
        return event


def test_copy(app):
    s = new_stream(app)
    s2 = copy(s)
    assert s2 is not s
    assert s2.channel is s.channel


def test_repr(app):
    assert repr(new_stream(app))


def test_repr__combined(app):
    assert repr(new_stream(app) & new_stream(app))


def test_label(app):
    assert label(new_stream(app))


def test_iter_raises(app):
    with pytest.raises(NotImplementedError):
        for _ in new_stream(app):
            assert False


def test_derive_topic_from_nontopic_channel_raises(app):
    with pytest.raises(ValueError):
        new_stream(app).derive_topic('bar')


@pytest.mark.asyncio
async def test_remove_from_stream(app):
    s = new_stream(app)
    await s.start()
    assert not s.should_stop
    await s.remove_from_stream(s)
    assert s.should_stop


@pytest.mark.asyncio
async def test_stop_stops_related_streams(app):
    s1 = new_stream(app)
    s2 = new_stream(app)
    await s1.start()
    await s2.start()
    s3 = s1 & s2
    await s3.stop()
    assert s1.should_stop
    assert s2.should_stop
    assert s3.should_stop


@pytest.mark.asyncio
async def test_take(app):
    async with new_stream(app) as s:
        assert s.enable_acks is True
        await s.channel.send(value=1)
        event = None
        async for value in s.take(1, within=1):
            assert value == [1]
            assert s.enable_acks is False
            event = mock_stream_event_ack(s)
            break

        assert event
        # need one sleep on Python 3.6.0-3.6.6 + 3.7.0
        # need two sleeps on Python 3.6.7 + 3.7.1 :-/
        await asyncio.sleep(0)  # needed for some reason
        await asyncio.sleep(0)  # needed for some reason

        if not event.ack.called:
            assert event.message.acked
            assert not event.message.refcount
        assert s.enable_acks is True


@pytest.mark.asyncio
async def test_send__to_non_topic_channel_stream(app):
    async with new_stream(app) as s:
        s.channel = [1, 2, 3]
        with pytest.raises(NotImplementedError):
            await s.send('foo')
