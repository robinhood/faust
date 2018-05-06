from unittest.mock import Mock, patch
import pytest
from faust.agents.agent import (
    Actor,
    Agent,
    AsyncIterableActor,
    AwaitableActor,
)
from faust.types import TP
from mode import label
from mode.utils.futures import done_future


class test_Actor:

    ActorType = Actor

    @pytest.fixture()
    def agent(self):
        agent = Mock(name='agent')
        agent.name = 'myagent'
        return agent

    @pytest.fixture()
    def stream(self):
        return Mock(name='stream')

    @pytest.fixture()
    def it(self):
        it = Mock(name='it')
        it.__aiter__ = Mock(name='it.__aiter__')
        it.__await__ = Mock(name='it.__await__')
        return it

    @pytest.fixture()
    def actor(self, *, agent, stream, it):
        return self.ActorType(agent, stream, it)

    def test_constructor(self, *, actor, agent, stream, it):
        assert actor.agent is agent
        assert actor.stream is stream
        assert actor.it is it
        assert actor.index is None
        assert actor.active_partitions is None
        assert actor.actor_task is None

    @pytest.mark.asyncio
    async def test_on_start(self, *, actor):
        actor.actor_task = Mock(name='actor_task')
        actor.add_future = Mock(name='add_future')
        await actor.on_start()
        actor.add_future.assert_called_once_with(actor.actor_task)

    @pytest.mark.asyncio
    async def test_on_stop(self, *, actor):
        actor.cancel = Mock(name='cancel')
        await actor.on_stop()
        actor.cancel.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_isolated_partition_revoked(self, *, actor):
        actor.cancel = Mock(name='cancel')
        actor.stop = Mock(name='stop')
        actor.stop.return_value = done_future()
        await actor.on_isolated_partition_revoked(TP('foo', 0))
        actor.cancel.assert_called_once_with()
        actor.stop.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_isolated_partition_assigned(self, *, actor):
        await actor.on_isolated_partition_assigned(TP('foo', 0))

    def test_cancel(self, *, actor):
        actor.actor_task = Mock(name='actor_task')
        actor.cancel()
        actor.actor_task.cancel.assert_called_once_with()

    def test_cancel__when_no_task(self, *, actor):
        actor.actor_task = None
        actor.cancel()

    def test_repr(self, *, actor):
        assert repr(actor)


class test_AsyncIterableActor(test_Actor):

    ActorType = AsyncIterableActor

    def test_aiter(self, *, actor, it):
        res = actor.__aiter__()
        it.__aiter__.assert_called_with()
        assert res is it.__aiter__()


class test_AwaitableActor(test_Actor):

    ActorType = AwaitableActor

    def test_await(self, *, actor, it):
        res = actor.__await__()
        it.__await__.assert_called_with()
        assert res is it.__await__()


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
