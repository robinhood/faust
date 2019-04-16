import asyncio
from collections import defaultdict
from time import monotonic
import faust
import pytest
from faust import joins
from mode.utils.contexts import ExitStack
from mode.utils.mocks import AsyncMock, Mock, patch
from t.helpers import new_event


class Model(faust.Record):
    foo: str


class test_Stream:

    @pytest.fixture()
    def stream(self, *, app):
        return app.stream(app.channel())

    def test_join(self, *, stream):
        s2 = stream.join(Model.foo)
        assert s2.join_strategy
        assert isinstance(s2.join_strategy, joins.RightJoin)
        assert s2.join_strategy.stream is stream
        assert s2.join_strategy.fields == {Model.foo.model: Model.foo}

    def test_left_join(self, *, stream):
        s2 = stream.left_join(Model.foo)
        assert s2.join_strategy
        assert isinstance(s2.join_strategy, joins.LeftJoin)
        assert s2.join_strategy.stream is stream
        assert s2.join_strategy.fields == {Model.foo.model: Model.foo}

    def test_inner_join(self, *, stream):
        s2 = stream.inner_join(Model.foo)
        assert s2.join_strategy
        assert isinstance(s2.join_strategy, joins.InnerJoin)
        assert s2.join_strategy.stream is stream
        assert s2.join_strategy.fields == {Model.foo.model: Model.foo}

    def test_outer_join(self, *, stream):
        s2 = stream.outer_join(Model.foo)
        assert s2.join_strategy
        assert isinstance(s2.join_strategy, joins.OuterJoin)
        assert s2.join_strategy.stream is stream
        assert s2.join_strategy.fields == {Model.foo.model: Model.foo}

    @pytest.mark.asyncio
    async def test_on_merge__with_join_strategy(self, *, stream):
        join = stream.join_strategy = Mock(process=AsyncMock())
        assert (await stream.on_merge('v')) is join.process.coro.return_value

    def test_combine__finalized(self, *, stream):
        stream._finalized = True
        assert stream.combine(Mock()) is stream

    def test_group_by__finalized(self, *, stream):
        stream._finalized = True
        assert stream.group_by(Mock()) is stream

    def test_through__finalized(self, *, stream):
        stream._finalized = True
        assert stream.through(Mock()) is stream

    def test__set_current_event(self, *, stream):
        with patch('faust.streams._current_event') as ce:
            stream._set_current_event(None)
            assert stream.current_event is None
            ce.set.assert_called_once_with(None)
            ce.set.reset_mock()

            with patch('weakref.ref') as ref:
                event = Mock(name='event')
                stream._set_current_event(event)
                assert stream.current_event is event
                ce.set.assert_called_once_with(ref.return_value)
                ref.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test__format_key(self, *, stream):
        value = Model('val')
        assert (await stream._format_key(Model.foo, value)) == 'val'

    @pytest.mark.asyncio
    async def test__format_key__callable(self, *, stream):
        async def format_key(value):
            return value.foo
        value = Model('val')
        assert (await stream._format_key(format_key, value)) == 'val'

    @pytest.mark.asyncio
    async def test_groupby__field_descriptor(self, *, stream, app):
        async with stream:
            channel = app.channel()
            stream.prefix = 'agent-foo'
            s2 = stream.group_by(Model.foo, topic=channel)

            assert not s2._processors
            assert len(stream._processors) == 1

            repartition = stream._processors[0]

            stream.current_event = None
            with pytest.raises(RuntimeError):
                await repartition(Model('foo'))

            stream.current_event = Mock(name='event', forward=AsyncMock())
            await repartition(Model('foo'))
            stream.current_event.forward.assert_called_once_with(
                channel, key='foo')

    @pytest.mark.asyncio
    async def test_echo(self, *, stream, app):
        channel = Mock(name='channel', send=AsyncMock())
        s2 = stream.echo(channel)
        assert len(s2._processors) == 1
        echoing = s2._processors[0]

        await echoing('val')
        channel.send.assert_called_once_with(value='val')

    @pytest.mark.asyncio
    @pytest.mark.allow_lingering_tasks(count=1)
    async def test_aiter_tracked(self, *, stream, app):
        event = await self._assert_stream_aiter(
            stream,
            side_effect=None,
            raises=None,
        )
        if not event.ack.called:
            assert event.message.refcount == 0
            assert event.message.acked
        else:
            event.ack.assert_called_once_with()

    @pytest.mark.asyncio
    @pytest.mark.allow_lingering_tasks(count=1)
    async def test_aiter_tracked__last_batch_set(self, *, stream, app):
        event = await self._assert_stream_aiter(
            stream,
            side_effect=None,
            raises=None,
            last_batch=monotonic(),
        )
        if not event.ack.called:
            assert event.message.refcount == 0
            assert event.message.acked
        else:
            event.ack.assert_called_once_with()

    @pytest.mark.asyncio
    @pytest.mark.allow_lingering_tasks(count=1)
    async def test_aiter_tracked__CancelledError(self, *, stream, app):
        event = await self._assert_stream_aiter(
            stream,
            side_effect=asyncio.CancelledError(),
            raises=asyncio.CancelledError,
        )
        if not event.ack.called:
            assert event.message.refcount == 0
            assert event.message.acked
        else:
            event.ack.assert_called_once_with()

    async def _assert_stream_aiter(self, stream,
                                   side_effect=None,
                                   raises=None,
                                   last_batch=None):
        sentinel = object()
        app = stream.app
        app.consumer = Mock(name='app.consumer')
        app.consumer._last_batch = last_batch
        app.consumer._committed_offset = defaultdict(lambda: -1)
        app.consumer._acked_index = defaultdict(set)
        app.consumer._acked = defaultdict(list)
        app.consumer._n_acked = 0
        app.flow_control.resume()
        app.topics._acking_topics.add('foo')
        event = new_event(app, topic='foo', value=32)
        event.message.tracked = False
        event.ack = Mock(name='event.ack')
        channel = app.channel()

        got_sentinel = asyncio.Event()

        @app.agent(channel)
        async def agent(stream):
            with ExitStack() as stack:
                if raises:
                    stack.enter_context(pytest.raises(raises))
                async for value in stream:
                    if value is sentinel:
                        got_sentinel.set()
                        break
                    assert value is event.value
                    if side_effect:
                        got_sentinel.set()
                        raise side_effect

        s: faust.StreamT = None
        async with agent as _agent:
            s = list(_agent._actors)[0].stream.get_active_stream()
            await s.channel.put(event)
            await s.channel.put(new_event(app, topic='bar', value=sentinel))
            s._on_message_in = Mock()
            await got_sentinel.wait()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        s._on_message_in.assert_called_once_with(
            event.message.tp,
            event.message.offset,
            event.message,
        )
        assert app.consumer._last_batch
        return event

    @pytest.mark.asyncio
    async def test_ack(self, *, stream):
        event = Mock()
        event.ack.return_value = True
        stream._on_stream_event_out = Mock()
        stream._on_message_out = Mock()
        assert await stream.ack(event)
        stream._on_stream_event_out.assert_called_once_with(
            event.message.tp,
            event.message.offset,
            stream,
            event,
        )
        stream._on_message_out.assert_called_once_with(
            event.message.tp,
            event.message.offset,
            event.message,
        )
