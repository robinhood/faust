import asyncio

import pytest
from faust import App, Channel, Record
from faust.agents.actor import Actor
from faust.agents.agent import Agent, AgentService
from faust.agents.models import ReqRepRequest, ReqRepResponse
from faust.agents.replies import ReplyConsumer
from faust.events import Event
from faust.exceptions import ImproperlyConfigured
from faust.types import Message, TP
from mode import SupervisorStrategy, label
from mode.utils.aiter import aiter
from mode.utils.futures import done_future
from mode.utils.logging import CompositeLogger
from mode.utils.mocks import ANY, AsyncMock, FutureMock, Mock, call, patch
from mode.utils.trees import Node


class Word(Record):
    word: str


class test_AgentService:

    @pytest.fixture
    def agent(self):
        return Mock(name='agent', autospec=Agent)

    @pytest.fixture
    def service(self, *, agent):
        return AgentService(agent)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('concurrency,index,expected_index', [
        (1, 3, None),
        (10, 3, 3),
    ])
    async def test_start_one(self, concurrency, index, expected_index, *,
                             agent, service):
        agent.concurrency = concurrency
        agent._start_task = AsyncMock(name='_start_task')

        tps = {TP('foo', 0)}
        await service._start_one(index=index, active_partitions=tps)
        agent._start_task.assert_called_once_with(
            expected_index, tps, None, service.beacon)

    @pytest.mark.asyncio
    async def test_start_for_partitions(self, *, service):
        service._start_one_supervised = AsyncMock(name='_start_one')
        s = service._start_one_supervised.return_value = Mock(name='service')
        s.maybe_start = AsyncMock(name='service.maybe_start')
        tps = {TP('foo', 3)}
        await service._start_for_partitions(tps)
        service._start_one_supervised.assert_called_once_with(None, tps)

    @pytest.mark.asyncio
    async def test_on_start(self, *, service):
        service._new_supervisor = Mock(name='new_supervisor')
        service._on_start_supervisor = AsyncMock(name='on_start_supervisor')
        await service.on_start()

        service._new_supervisor.assert_called_once_with()
        assert service.supervisor is service._new_supervisor()
        service._on_start_supervisor.assert_called_once_with()

    def test_new_supervisor(self, *, service):
        strategy = service._get_supervisor_strategy = Mock(name='strategy')
        s = service._new_supervisor()
        strategy.assert_called_once_with()
        strategy.return_value.assert_called_once_with(
            max_restarts=100.0,
            over=1.0,
            replacement=service._replace_actor,
            loop=service.loop,
            beacon=service.beacon,
        )
        assert s is strategy()()

    def test_get_supervisor_strategy(self, *, service, agent):
        agent.supervisor_strategy = 100
        assert service._get_supervisor_strategy() == 100
        agent.supervisor_strategy = None
        agent.app = Mock(name='app', autospec=App)
        assert (service._get_supervisor_strategy() is
                agent.app.conf.agent_supervisor)

    @pytest.mark.asyncio
    async def test_on_start_supervisor(self, *, service, agent):
        agent.concurrency = 10
        service._get_active_partitions = Mock(name='_get_active_partitions')
        service._start_one = AsyncMock(name='_start_one')
        service.supervisor = Mock(
            name='supervisor',
            autospec=SupervisorStrategy,
            start=AsyncMock(),
        )
        await service._on_start_supervisor()

        service._start_one.coro.assert_has_calls([
            call(i, service._get_active_partitions()) for i in range(10)
        ])
        service.supervisor.add.assert_has_calls([
            call(service._start_one.coro()) for i in range(10)
        ])
        service.supervisor.start.assert_called_once_with()

    def test_get_active_partitions(self, *, service, agent):
        agent.isolated_partitions = None
        assert service._get_active_partitions() is None
        agent.isolated_partitions = True
        assert service._get_active_partitions() == set()
        assert agent._pending_active_partitions == set()

    @pytest.mark.asyncio
    async def test_replace_actor(self, *, service):
        aref = Mock(name='aref', autospec=Actor)
        service._start_one = AsyncMock(name='_start_one')
        assert (await service._replace_actor(aref, 101) ==
                service._start_one.coro())
        service._start_one.assert_called_once_with(
            101,
            aref.active_partitions,
            aref.stream,
        )

    @pytest.mark.asyncio
    async def test_on_stop(self, *, service):
        service._stop_supervisor = AsyncMock(name='_stop_supervisor')
        await service.on_stop()
        service._stop_supervisor.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_stop_supervisor(self, *, service):
        supervisor = service.supervisor = Mock(
            name='supervisor',
            autospec=SupervisorStrategy,
            stop=AsyncMock(),
        )
        await service._stop_supervisor()
        assert service.supervisor is None
        supervisor.stop.assert_called_once_with()
        await service._stop_supervisor()
        supervisor.stop.assert_called_once_with()

    def test_label(self, *, service):
        assert label(service)


class test_Agent:

    @pytest.fixture
    def agent(self, *, app):

        @app.agent()
        async def myagent(stream):
            async for value in stream:
                yield value

        return myagent

    @pytest.fixture
    def isolated_agent(self, *, app):

        @app.agent(isolated_partitions=True)
        async def isoagent(stream):
            async for value in stream:
                yield value

        return isoagent

    @pytest.fixture
    def foo_topic(self, *, app):
        return app.topic('foo')

    @pytest.fixture
    def agent2(self, *, app, foo_topic):

        @app.agent(foo_topic)
        async def other_agent(stream):
            async for value in stream:
                value

        return other_agent

    def test_init_key_type_and_channel(self, *, app):
        with pytest.raises(AssertionError):
            @app.agent(app.topic('foo'), key_type=bytes)
            async def foo():
                ...

    def test_init_value_type_and_channel(self, *, app):
        with pytest.raises(AssertionError):
            @app.agent(app.topic('foo'), value_type=bytes)
            async def foo():
                ...

    def test_isolated_partitions_cannot_have_concurrency(self, *, app):
        with pytest.raises(ImproperlyConfigured):
            @app.agent(isolated_partitions=True, concurrency=100)
            async def foo():
                ...

    def test_cancel(self, *, agent):
        actor1 = Mock(name='actor1')
        actor2 = Mock(name='actor2')
        agent._actors = [actor1, actor2]
        agent.cancel()
        actor1.cancel.assert_called_once_with()
        actor2.cancel.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, agent):
        revoked = {TP('foo', 0)}
        agent.on_shared_partitions_revoked = AsyncMock(name='ospr')
        await agent.on_partitions_revoked(revoked)
        agent.on_shared_partitions_revoked.assert_called_once_with(revoked)

    @pytest.mark.asyncio
    async def test_on_partitions_revoked__isolated(self, *, isolated_agent):
        revoked = {TP('foo', 0)}
        i = isolated_agent.on_isolated_partitions_revoked = AsyncMock(name='i')
        await isolated_agent.on_partitions_revoked(revoked)
        i.assert_called_once_with(revoked)

    @pytest.mark.asyncio
    async def test_on_partitions_assigned(self, *, agent):
        assigned = {TP('foo', 0)}
        agent.on_shared_partitions_assigned = AsyncMock(name='ospr')
        await agent.on_partitions_assigned(assigned)
        agent.on_shared_partitions_assigned.assert_called_once_with(assigned)

    @pytest.mark.asyncio
    async def test_on_partitions_assigned__isolated(self, *, isolated_agent):
        assigned = {TP('foo', 0)}
        i = isolated_agent.on_isolated_partitions_assigned = AsyncMock()
        await isolated_agent.on_partitions_assigned(assigned)
        i.assert_called_once_with(assigned)

    @pytest.mark.asyncio
    async def test_on_isolated_partitions_revoked(self, *, agent):
        tp = TP('foo', 0)
        aref = Mock(
            name='aref',
            autospec=Actor,
            on_isolated_partition_revoked=AsyncMock(),
        )
        agent._actor_by_partition = {tp: aref}

        await agent.on_isolated_partitions_revoked({tp})
        aref.on_isolated_partition_revoked.assert_called_once_with(tp)
        assert not agent._actor_by_partition
        await agent.on_isolated_partitions_revoked({tp})

    @pytest.mark.asyncio
    async def test_on_isolated_partitions_assigned(self, *, agent):
        agent._assign_isolated_partition = AsyncMock(name='aip')
        await agent.on_isolated_partitions_assigned({TP('foo', 0)})
        agent._assign_isolated_partition.assert_called_once_with(TP('foo', 0))

    @pytest.mark.asyncio
    async def test_assign_isolated_partition(self, *, agent):
        agent._on_first_isolated_partition_assigned = Mock(name='ofipa')
        agent._maybe_start_isolated = AsyncMock(name='maybe_start_isolated')
        agent._first_assignment_done = True

        tp = TP('foo', 606)
        await agent._assign_isolated_partition(tp)

        agent._on_first_isolated_partition_assigned.assert_not_called()
        agent._maybe_start_isolated.assert_called_once_with(tp)

        agent._first_assignment_done = False
        agent._actor_by_partition = set()
        await agent._assign_isolated_partition(tp)
        agent._on_first_isolated_partition_assigned.assert_called_once_with(tp)

    def test_on_first_isolated_partition_assigned(self, *, agent):
        aref = Mock(name='actor', autospec=Actor)
        agent._actors = [aref]
        agent._pending_active_partitions = set()
        tp = TP('foo', 303)
        agent._on_first_isolated_partition_assigned(tp)
        assert agent._actor_by_partition[tp] is aref
        assert agent._pending_active_partitions == {tp}
        agent._pending_active_partitions = None
        agent._on_first_isolated_partition_assigned(tp)

    @pytest.mark.asyncio
    async def test_maybe_start_isolated(self, *, isolated_agent):
        aref = Mock(
            name='actor',
            autospec=Actor,
            on_isolated_partition_assigned=AsyncMock(),
        )
        isolated_agent._start_isolated = AsyncMock(
            name='_start_isolated',
            return_value=aref,
        )
        tp = TP('foo', 303)
        await isolated_agent._maybe_start_isolated(tp)

        isolated_agent._start_isolated.assert_called_once_with(tp)
        assert isolated_agent._actor_by_partition[tp] is aref
        aref.on_isolated_partition_assigned.assert_called_once_with(tp)

    @pytest.mark.asyncio
    async def test_start_isolated(self, *, agent):
        service = agent._service = Mock(
            name='service',
            autospec=AgentService,
            _start_for_partitions=AsyncMock(),
        )
        ret = await agent._start_isolated(TP('foo', 0))
        service._start_for_partitions.assert_called_once_with({TP('foo', 0)})
        assert ret is service._start_for_partitions.coro()

    @pytest.mark.asyncio
    async def test_on_shared_partitions_revoked(self, *, agent):
        await agent.on_shared_partitions_revoked(set())

    @pytest.mark.asyncio
    async def test_on_shared_partitions_assigned(self, *, agent):
        await agent.on_shared_partitions_assigned(set())

    def test_info(self, *, agent):
        assert agent.info() == {
            'app': agent.app,
            'fun': agent.fun,
            'name': agent.name,
            'channel': agent.channel,
            'concurrency': agent.concurrency,
            'help': agent.help,
            'sinks': agent._sinks,
            'on_error': agent._on_error,
            'supervisor_strategy': agent.supervisor_strategy,
            'isolated_partitions': agent.isolated_partitions,
        }

    def test_clone(self, *, agent):
        assert agent.clone(isolated_partitions=True).isolated_partitions

    def test_stream__active_partitions(self, *, agent):
        assert agent.stream(active_partitions={TP('foo', 0)})

    @pytest.mark.parametrize('input,expected', [
        (ReqRepRequest('value', 'reply_to', 'correlation_id'), 'value'),
        ('value', 'value'),
    ])
    def test_maybe_unwrap_reply_request(self, input, expected, *, agent):
        assert agent._maybe_unwrap_reply_request(input) == expected

    @pytest.mark.asyncio
    async def test_start_task(self, *, agent):
        agent._prepare_actor = AsyncMock(name='_prepare_actor')
        ret = await agent._start_task(index=0)
        agent._prepare_actor.assert_called_once_with(ANY, None)
        assert ret is agent._prepare_actor.coro()

    @pytest.mark.asyncio
    async def test_prepare_actor__AsyncIterable(self, *, agent):
        aref = agent(index=0, active_partitions=None)
        with patch('asyncio.Task') as Task:
            agent._slurp = Mock(name='_slurp')
            agent._execute_task = Mock(name='_execute_task')
            beacon = Mock(name='beacon', autospec=Node)
            ret = await agent._prepare_actor(aref, beacon)
            agent._slurp.assert_called()
            coro = agent._slurp()
            agent._execute_task.assert_called_once_with(coro, aref)
            Task.assert_called_once_with(
                agent._execute_task(), loop=agent.loop)
            task = Task()
            assert task._beacon is beacon
            assert aref.actor_task is task
            assert aref in agent._actors
            assert ret is aref

    @pytest.mark.asyncio
    async def test_prepare_actor__Awaitable(self, *, agent2):
        aref = agent2(index=0, active_partitions=None)
        asyncio.ensure_future(aref.it).cancel()  # silence warning
        return
        with patch('asyncio.Task') as Task:
            agent2._execute_task = Mock(name='_execute_task')
            beacon = Mock(name='beacon', autospec=Node)
            ret = await agent2._prepare_actor(aref, beacon)
            coro = aref
            agent2._execute_task.assert_called_once_with(coro, aref)
            Task.assert_called_once_with(
                agent2._execute_task(), loop=agent2.loop)
            task = Task()
            assert task._beacon is beacon
            assert aref.actor_task is task
            assert aref in agent2._actors
            assert ret is aref

    @pytest.mark.asyncio
    async def test_prepare_actor__Awaitable_cannot_have_sinks(self, *, agent2):
        aref = agent2(index=0, active_partitions=None)
        asyncio.ensure_future(aref.it).cancel()  # silence warning
        agent2._sinks = [agent2]
        with pytest.raises(ImproperlyConfigured):
            await agent2._prepare_actor(
                aref,
                Mock(name='beacon', autospec=Node),
            )

    @pytest.mark.asyncio
    async def test_execute_task(self, *, agent):
        coro = done_future()
        await agent._execute_task(coro, Mock(name='aref', autospec=Actor))

    @pytest.mark.asyncio
    async def test_execute_task__cancelled_stopped(self, *, agent):
        coro = FutureMock()
        coro.side_effect = asyncio.CancelledError()
        await agent.stop()
        with pytest.raises(asyncio.CancelledError):
            await agent._execute_task(coro, Mock(name='aref', autospec=Actor))
        coro.assert_awaited()

    @pytest.mark.asyncio
    async def test_execute_task__cancelled_running(self, *, agent):
        coro = FutureMock()
        coro.side_effect = asyncio.CancelledError()
        await agent._execute_task(coro, Mock(name='aref', autospec=Actor))
        coro.assert_awaited()

    @pytest.mark.asyncio
    async def test_execute_task__raising(self, *, agent):
        agent._on_error = AsyncMock(name='on_error')
        agent.log = Mock(name='log', autospec=CompositeLogger)
        aref = Mock(
            name='aref',
            autospec=Actor,
            crash=AsyncMock(),
        )
        agent._service = Mock(name='_service', autospec=AgentService)
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
        agent._delegate_to_sinks = AsyncMock(name='_delegate_to_sinks')
        agent._reply = AsyncMock(name='_reply')

        def on_delegate(value):
            raise StopAsyncIteration()

        word = Word('word')
        word_req = ReqRepRequest(word, 'reply_to', 'correlation_id')
        message1 = Mock(name='message1', autospec=Message)
        message2 = Mock(name='message2', autospec=Message)
        event1 = Event(app, None, word_req, message1)
        event2 = Event(app, 'key', 'bar', message2)
        values = [
            (event1, word),
            (event2, 'bar'),
        ]

        class AIT:

            async def __aiter__(self):
                for event, value in values:
                    stream.current_event = event
                    yield value
        it = aiter(AIT())
        await agent._slurp(aref, it)

        agent._reply.assert_called_once_with(None, word, word_req)
        agent._delegate_to_sinks.coro.assert_has_calls([
            call(word),
            call('bar'),
        ])

    @pytest.mark.asyncio
    async def test_delegate_to_sinks(self, *, agent, agent2, foo_topic):
        agent2.send = AsyncMock(name='agent2.send')
        foo_topic.send = AsyncMock(name='foo_topic.send')
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
        agent.app = Mock(
            name='app',
            autospec=App,
            send=AsyncMock(),
        )
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
        agent.send = AsyncMock(name='send')
        await agent.cast('value', key='key', partition=303)
        agent.send.assert_called_once_with('key', 'value', partition=303)

    @pytest.mark.asyncio
    async def test_ask(self, *, agent):
        agent.app = Mock(
            name='app',
            autospec=App,
            maybe_start_client=AsyncMock(),
            _reply_consumer=Mock(
                autospec=ReplyConsumer,
                add=AsyncMock(),
            ),
        )
        pp = done_future()
        agent.ask_nowait = Mock(name='ask_nowait')
        agent.ask_nowait.return_value = done_future(pp)
        pp.correlation_id = 'foo'

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
        agent.channel.send = AsyncMock(name='channel.send')
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
        agent.channel = Mock(
            name='channel',
            autospec=Channel,
            send=AsyncMock(),
        )
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

        assert ret is agent.channel.send.coro()

    @pytest.mark.asyncio
    async def test_send__without_reply_to(self, *, agent):
        agent.channel = Mock(
            name='channel',
            autospec=Channel,
            send=AsyncMock(),
        )
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

        assert ret is agent.channel.send.coro()

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

    def test_prepare_channel__not_channel(self, *, agent):
        with pytest.raises(TypeError):
            agent._prepare_channel(object())

    def test_add_sink(self, *, agent, agent2):
        agent.add_sink(agent2)
        assert agent2 in agent._sinks
        agent.add_sink(agent2)

    def test_channel_iterator(self, *, agent):
        agent.channel = Mock(name='channel', autospec=Channel)
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
