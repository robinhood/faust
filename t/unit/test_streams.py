import faust
import pytest
from faust import joins
from mode.utils.mocks import AsyncMock, Mock, patch


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
