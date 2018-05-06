import asyncio
from unittest.mock import Mock, call, patch

import pytest
from faust.agents.agent import (
    Actor,
    Agent,
    AsyncIterableActor,
    AwaitableActor,
)
from faust import Record
from faust.agents.models import ReqRepRequest, ReqRepResponse
from faust.events import Event
from faust.types import TP
from mode import label
from mode.utils.aiter import aiter
from mode.utils.futures import done_future


class Word(Record):
    word: str


class FutureMock(Mock):
    awaited = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._loop = asyncio.get_event_loop()

    def __await__(self):
        self.awaited = True
        yield self()

    def assert_awaited(self):
        assert self.awaited

    def assert_not_awaited(self):
        assert not self.awaited


class test_Agent:

    @pytest.fixture
    def agent(self, *, app):

        @app.agent()
        async def myagent(stream):
            async for value in stream:
                yield value

        return myagent

    @pytest.fixture
    def foo_topic(self, *, app):
        return app.topic('foo')

    @pytest.fixture
    def agent2(self, *, app, foo_topic):

        @app.agent(foo_topic)
        async def other_agent(stream):
            async for value in stream:
                ...

        return other_agent

    @pytest.mark.asyncio
    async def test_execute_task(self, *, agent):
        coro = done_future()
        await agent._execute_task(coro, Mock(name='aref'))

    @pytest.mark.asyncio
    async def test_execute_task__cancelled_stopped(self, *, agent):
        coro = FutureMock()
        coro.side_effect = asyncio.CancelledError()
        await agent.stop()
        with pytest.raises(asyncio.CancelledError):
            await agent._execute_task(coro, Mock(name='aref'))
        coro.assert_awaited()

    @pytest.mark.asyncio
    async def test_execute_task__cancelled_running(self, *, agent):
        coro = FutureMock()
        coro.side_effect = asyncio.CancelledError()
        await agent._execute_task(coro, Mock(name='aref'))
        coro.assert_awaited()

    @pytest.mark.asyncio
    async def test_execute_task__raising(self, *, agent):
        agent._on_error = Mock(name='on_error')
        agent._on_error.return_value = done_future()
        agent.log = Mock(name='log')
        aref = Mock(name='aref')
        aref.crash.return_value = done_future()
        agent._service = Mock(name='_service')
        coro = FutureMock()
        exc = coro.side_effect = KeyError('bar')
        with pytest.raises(KeyError):
            await agent._execute_task(coro, aref)
        coro.assert_awaited()

        aref.crash.assert_called_once_with(exc)
        agent._service.supervisor.wakeup.assert_called_once_with()
        agent._on_error.assert_called_once_with(agent, exc)

        agent._on_error = None
        with pytest.raises(KeyError):
            await agent._execute_task(coro, aref)

    @pytest.mark.asyncio
    async def test_slurp(self, *, agent, app):
        aref = agent(index=None, active_partitions=None)
        stream = aref.stream.get_active_stream()
        agent._delegate_to_sinks = Mock(name='_delegate_to_sinks')
        agent._delegate_to_sinks.return_value = done_future()
        agent._reply = Mock(name='_reply')
        agent._reply.return_value = done_future()

        def on_delegate(value):
            raise StopAsyncIteration()

        word = Word('word')
        word_req = ReqRepRequest(word, 'reply_to', 'correlation_id')
        values = [
            (Event(app, None, word_req, Mock(name='message1')), word),
            (Event(app, 'key', 'bar', Mock(name='message2')), 'bar'),
        ]

        class AIT:

            async def __aiter__(self):
                for event, value in values:
                    stream.current_event = event
                    yield value
        it = aiter(AIT())
        await agent._slurp(aref, it)

        agent._reply.assert_called_once_with(None, word, word_req)
        agent._delegate_to_sinks.assert_has_calls([
            call(word),
            call('bar'),
        ])

    @pytest.mark.asyncio
    async def test_delegate_to_sinks(self, *, agent, agent2, foo_topic):
        agent2.send = Mock(name='agent2.send')
        agent2.send.return_value = done_future()
        foo_topic.send = Mock(name='foo_topic.send')
        foo_topic.send.return_value = done_future()
        sink_callback = Mock(name='sink_callback')
        sink_callback2_mock = Mock(name='sink_callback2_mock')

        async def sink_callback2(value):
            return sink_callback2_mock(value)

        agent._sinks = [
            agent2,
            foo_topic,
            sink_callback,
            sink_callback2,
        ]

        value = Mock(name='value')
        await agent._delegate_to_sinks(value)

        agent2.send.assert_called_once_with(value=value)
        foo_topic.send.assert_called_once_with(value=value)
        sink_callback.assert_called_once_with(value)
        sink_callback2_mock.assert_called_once_with(value)

    @pytest.mark.asyncio
    async def test_reply(self, *, agent):
        agent.app = Mock(name='app')
        agent.app.send.return_value = done_future()
        req = ReqRepRequest('value', 'reply_to', 'correlation_id')
        await agent._reply('key', 'reply', req)
        agent.app.send.assert_called_once_with(
            req.reply_to,
            key=None,
            value=ReqRepResponse(
                key='key',
                value='reply',
                correlation_id=req.correlation_id,
            ),
        )


    @pytest.mark.asyncio
    async def test_cast(self, *, agent):
        agent.send = Mock(name='send')
        agent.send.return_value = done_future()
        await agent.cast('value', key='key', partition=303)
        agent.send.assert_called_once_with('key', 'value', partition=303)

    @pytest.mark.asyncio
    async def test_ask(self, *, agent):
        agent.app = Mock(name='app')
        agent.ask_nowait = Mock(name='ask_nowait')
        pp = done_future()
        p = agent.ask_nowait.return_value = done_future(pp)
        pp.correlation_id = 'foo'
        agent.app._reply_consumer.add.return_value = done_future()
        agent.app.maybe_start_client.return_value = done_future()

        await agent.ask(
            value='val',
            key='key',
            partition=303,
            correlation_id='correlation_id',
        )
        agent.ask_nowait.assert_called_once_with(
            'val',
            key='key',
            partition=303,
            reply_to=agent.app.conf.reply_to,
            correlation_id='correlation_id',
            force=True,
        )
        agent.app._reply_consumer.add.assert_called_once_with(
            pp.correlation_id, pp)

    @pytest.mark.asyncio
    async def test_ask_nowait(self, *, agent):
        agent._create_req = Mock(name='_create_req')
        agent.channel.send = Mock(name='channel.send')
        agent.channel.send.return_value = done_future()
        res = await agent.ask_nowait(
            value='value',
            key='key',
            partition=303,
            reply_to='reply_to',
            correlation_id='correlation_id',
            force=True,
        )

        agent._create_req.assert_called_once_with(
            'key', 'value', 'reply_to', 'correlation_id')
        agent.channel.send.assert_called_once_with(
            'key', agent._create_req(), 303, force=True)

        assert res.reply_to == agent._create_req().reply_to
        assert res.correlation_id == agent._create_req().correlation_id

    def test_create_req(self, *, agent):
        agent._get_strtopic = Mock(name='_get_strtopic')
        with patch('faust.agents.agent.uuid4') as uuid4:
            uuid4.return_value = 'vvv'
            reqrep = agent._create_req(
                key=b'key', value=b'value', reply_to='reply_to')

            agent._get_strtopic.assert_called_once_with('reply_to')

            assert reqrep.value == b'value'
            assert reqrep.reply_to == agent._get_strtopic()
            assert reqrep.correlation_id == 'vvv'

    @pytest.mark.asyncio
    async def test_send(self, *, agent):
        agent.channel = Mock(name='channel')
        agent.channel.send.return_value = done_future('sent')
        agent._create_req = Mock(name='_create_req')
        callback = Mock(name='callback')

        ret = await agent.send(
            key=b'key',
            value=b'value',
            partition=303,
            key_serializer='raw',
            value_serializer='raw',
            callback=callback,
            reply_to='reply_to',
            correlation_id='correlation_id',
            force=True,
        )

        agent._create_req.assert_called_once_with(
            b'key', b'value', 'reply_to', 'correlation_id',
        )

        agent.channel.send.assert_called_once_with(
            b'key',
            agent._create_req(),
            303,
            'raw',
            'raw',
            force=True,
        )

        assert ret is 'sent'

    @pytest.mark.asyncio
    async def test_send__without_reply_to(self, *, agent):
        agent.channel = Mock(name='channel')
        agent.channel.send.return_value = done_future('sent')
        agent._create_req = Mock(name='_create_req')
        callback = Mock(name='callback')

        ret = await agent.send(
            key=b'key',
            value=b'value',
            partition=303,
            key_serializer='raw',
            value_serializer='raw',
            callback=callback,
            reply_to=None,
            correlation_id='correlation_id',
            force=True,
        )

        agent._create_req.assert_not_called()

        agent.channel.send.assert_called_once_with(
            b'key',
            b'value',
            303,
            'raw',
            'raw',
            force=True,
        )

        assert ret is 'sent'

    def test_get_strtopic__agent(self, *, agent, agent2):
        assert agent._get_strtopic(agent2) == agent2.channel.get_topic_name()

    def test_get_strtopic__topic(self, *, agent, foo_topic):
        assert agent._get_strtopic(foo_topic) == foo_topic.get_topic_name()

    def test_get_strtopic__str(self, *, agent):
        assert agent._get_strtopic('bar') == 'bar'

    def test_get_strtopic__channel_raises(self, *, agent, app):
        with pytest.raises(ValueError):
            agent._get_strtopic(app.channel())

    def test_get_topic_names(self, *, agent, app):
        agent.channel = app.topic('foo')
        assert agent.get_topic_names() == ('foo',)

    def test_get_topic_names__channel(self, *, agent, app):
        agent.channel = app.channel()
        assert agent.get_topic_names() == []

    def test_repr(self, *, agent):
        assert repr(agent)

    def test_channel(self, *, agent):
        agent._prepare_channel = Mock(name='_prepare_channel')
        agent._channel = None
        channel = agent.channel
        agent._prepare_channel.assert_called_once_with(
            agent._channel_arg,
            key_type=agent._key_type,
            value_type=agent._value_type,
            **agent._channel_kwargs)
        assert channel is agent._prepare_channel.return_value
        assert agent._channel is channel

    def test_channel_iterator(self, *, agent):
        agent.channel = Mock(name='channel')
        agent._channel_iterator = None
        it = agent.channel_iterator

        agent.channel.clone.assert_called_once_with(is_iterator=True)
        assert it is agent.channel.clone()
        agent.channel_iterator = [42]
        assert agent.channel_iterator == [42]

    def test_service(self, *, agent):
        with patch('faust.agents.agent.AgentService') as AgentService:
            service = agent._service
            AgentService.assert_called_once_with(
                agent,
                beacon=agent.app.beacon,
                loop=agent.app.loop,
            )
            assert service is AgentService()

    def test_label(self, *, agent):
        assert label(agent)
