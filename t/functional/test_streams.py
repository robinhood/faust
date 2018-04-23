import asyncio
from mode.utils.aiter import aiter, anext
import pytest
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


async def _start_stop_stream(stream):
    await stream.start()
    assert stream._prev._passive
    stream.__aiter__()
    assert stream.app.topics

    await stream.stop()
    assert not stream._prev._passive


@pytest.mark.asyncio
async def test_start_and_stop_Stream(app):
    s = new_topic_stream(app).group_by(lambda k: k, name='foo-bar')
    await _start_stop_stream(s)
    assert not app.topics._topics
    await app.producer.stop()
