import re
import collections
import faust
from faust.agents import Agent
from faust.app.base import SCAN_AGENT, SCAN_PAGE, SCAN_TASK
from faust.assignor.leader_assignor import LeaderAssignor, LeaderAssignorT
from faust.channels import Channel, ChannelT
from faust.cli.base import AppCommand
from faust.exceptions import (
    AlreadyConfiguredWarning,
    ConsumerNotStarted,
    ImproperlyConfigured,
    SameNode,
)
from faust.fixups.base import Fixup
from faust.sensors.monitor import Monitor
from faust.serializers import codecs
from faust.transport.base import Transport
from faust.transport.conductor import Conductor
from faust.transport.consumer import Consumer, Fetcher
from faust.types import TP
from faust.types.models import ModelT
from faust.types.settings import Settings
from faust.types.web import ResourceOptions
from mode import Service
from mode.utils.compat import want_bytes
from mode.utils.mocks import ANY, AsyncMock, Mock, call, patch
from yarl import URL
import pytest

TEST_TOPIC = 'test'
CONFIG_DICT = {
    'broker': 'kafka://foo',
    'stream_buffer_maxsize': 1,
}
CONFIG_PATH = 't.unit.app.test_base.ConfigClass'

TP1 = TP('foo', 0)
TP2 = TP('bar', 1)
TP3 = TP('baz', 2)
TP4 = TP('xuz', 3)


class ConfigClass:
    broker = 'kafka://foo'
    stream_buffer_maxsize = 1


class Key(faust.Record):
    value: int


class Value(faust.Record, serializer='json'):
    amount: float


@pytest.mark.asyncio
@pytest.mark.parametrize('key,topic_name,expected_topic,key_serializer', [
    ('key', TEST_TOPIC, TEST_TOPIC, None),
    (Key(value=10), TEST_TOPIC, TEST_TOPIC, None),
    ({'key': 'k'}, TEST_TOPIC, TEST_TOPIC, 'json'),
    (None, 'topic', 'topic', None),
    (b'key', TEST_TOPIC, TEST_TOPIC, None),
    ('key', 'topic', 'topic', None),
])
async def test_send(
        key, topic_name, expected_topic, key_serializer, app):
    topic = app.topic(topic_name)
    event = Value(amount=0.0)
    await app.send(topic, key, event, key_serializer=key_serializer)
    # do it twice so producer_started is also True
    await app.send(topic, key, event, key_serializer=key_serializer)
    expected_sender = app.producer.send
    if key is not None:
        if isinstance(key, str):
            # Default serializer is raw, and str should be serialized
            # (note that a bytes key will be left alone.
            key_serializer = 'raw'
        if isinstance(key, ModelT):
            expected_key = key.dumps(serializer='raw')
        elif key_serializer:
            expected_key = codecs.dumps(key_serializer, key)
        else:
            expected_key = want_bytes(key)
    else:
        expected_key = None
    expected_sender.assert_called_with(
        expected_topic, expected_key, event.dumps(),
        partition=None,
        timestamp=None,
        headers={},
    )


@pytest.mark.asyncio
async def test_send_str(app):
    await app.send('foo', Value(amount=0.0))


class test_App:

    def test_stream(self, *, app):
        s = app.topic(TEST_TOPIC).stream()
        assert s.channel.topics == (TEST_TOPIC,)
        assert s.channel in app.topics
        assert s.channel.app == app

    def test_new_producer(self, *, app):
        app._producer = None
        transport = app._transport = Mock(
            name='transport',
            autospec=Transport,
        )
        assert app._new_producer() is transport.create_producer.return_value
        transport.create_producer.assert_called_with(beacon=ANY)
        assert app.producer is transport.create_producer.return_value

    def test_new_transport(self, *, app, patching):
        by_url = patching('faust.transport.by_url')
        assert app._new_transport() is by_url.return_value.return_value
        assert app.transport is by_url.return_value.return_value
        by_url.assert_called_with(app.conf.broker[0])
        by_url.return_value.assert_called_with(
            app.conf.broker, app, loop=app.loop)
        app.transport = 10
        assert app.transport == 10

    @pytest.mark.asyncio
    async def test_on_stop(self, *, app):
        app._http_client = Mock(name='http_client', close=AsyncMock())
        app._producer = Mock(name='producer', flush=AsyncMock())
        await app.on_stop()
        app._http_client.close.assert_called_once_with()
        app._http_client = None
        await app.on_stop()
        app._producer = None
        await app.on_stop()

    @pytest.mark.asyncio
    async def test_stop_consumer__wait_empty_enabled(self, *, app):
        app.conf.stream_wait_empty = True
        await self.assert_stop_consumer(app)
        app._consumer.wait_empty.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_stop_consumer__wait_empty_disabled(self, *, app):
        app.conf.stream_wait_empty = False
        await self.assert_stop_consumer(app)
        app.consumer.wait_empty.assert_not_called()

    async def assert_stop_consumer(self, app):
        consumer = app._consumer = Mock(
            wait_empty=AsyncMock(),
        )
        consumer.assignment.return_value = set()
        app.tables = Mock()
        app.flow_control = Mock()
        app._stop_fetcher = AsyncMock()
        await app._stop_consumer()

        consumer.assignment.side_effect = ConsumerNotStarted()
        await app._stop_consumer()

        consumer.assignment.side_effect = None
        assigned = {TP('foo', 0), TP('bar', 1)}
        consumer.assignment.return_value = assigned

        await app._stop_consumer()
        app.tables.on_partitions_revoked.assert_called_once_with(assigned)
        consumer.stop_flow.assert_called_once_with()
        app.flow_control.suspend.assert_called_once_with()
        app._stop_fetcher.assert_called_once_with()

    def test_on_rebalance_start_end(self, *, app):
        app.tables = Mock()
        assert not app.rebalancing

        app.on_rebalance_start()
        assert app.rebalancing
        app.tables.on_rebalance_start.assert_called_once_with()

        app.on_rebalance_end()
        assert not app.rebalancing

        app.tracer = Mock(name='tracer')
        app.on_rebalance_start()
        span = app._rebalancing_span
        assert span is not None
        app.on_rebalance_end()
        span.finish.assert_called_once_with()

    def test_trace(self, *, app):
        app.tracer = None
        with app.trace('foo'):
            pass
        app.tracer = Mock()
        assert app.trace('foo') is app.tracer.trace.return_value

    def test_traced(self, *, app):
        @app.traced
        def foo(val):
            return val
        assert foo(42) == 42

    def test__start_span_from_rebalancing(self, *, app):
        app.tracer = None
        app._rebalancing_span = None
        assert app._start_span_from_rebalancing('foo')
        app.tracer = Mock(name='tracer')
        try:
            app._rebalancing_span = Mock(name='span')
            assert app._start_span_from_rebalancing('foo')
        finally:
            app.tracer = None
            app._rebalancing_span = None

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, app):
        app.on_partitions_revoked = Mock(send=AsyncMock())
        consumer = app.consumer = Mock(
            wait_empty=AsyncMock(),
            transactions=Mock(
                on_partitions_revoked=AsyncMock(),
            ),
        )
        app.tables = Mock()
        app.flow_control = Mock()
        app._producer = Mock(flush=AsyncMock())
        revoked = {TP('foo', 0), TP('bar', 1)}
        ass = app.consumer.assignment.return_value = {TP('foo', 0)}

        app.in_transaction = False
        await app._on_partitions_revoked(revoked)

        app.on_partitions_revoked.send.assert_called_once_with(revoked)
        consumer.stop_flow.assert_called_once_with()
        app.flow_control.suspend.assert_called_once_with()
        consumer.pause_partitions.assert_called_once_with(ass)
        app.flow_control.clear.assert_called_once_with()
        consumer.wait_empty.assert_called_once_with()
        app._producer.flush.assert_called_once_with()
        consumer.transactions.on_partitions_revoked.assert_not_called()

        app.in_transaction = True
        await app._on_partitions_revoked(revoked)
        consumer.transactions.on_partitions_revoked.assert_called_once_with(
            revoked)

    @pytest.mark.asyncio
    async def test_on_partitions_revoked__no_assignment(self, *, app):
        app.on_partitions_revoked = Mock(send=AsyncMock())
        app.consumer = Mock()
        app.tables = Mock()
        revoked = {TP('foo', 0), TP('bar', 1)}
        app.consumer.assignment.return_value = set()
        await app._on_partitions_revoked(revoked)

        app.on_partitions_revoked.send.assert_called_once_with(revoked)

    @pytest.mark.asyncio
    async def test_on_partitions_revoked__crashes(self, *, app):
        app.on_partitions_revoked = Mock(send=AsyncMock())
        app.crash = AsyncMock()
        app.consumer = Mock()
        app.tables = Mock()
        revoked = {TP('foo', 0), TP('bar', 1)}
        app.consumer.assignment.side_effect = RuntimeError()
        await app._on_partitions_revoked(revoked)

        app.crash.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_partitions_revoked__when_stopped(self, *, app):
        app._stopped.set()
        app._on_rebalance_when_stopped = AsyncMock()
        await app._on_partitions_revoked(set())
        app._on_rebalance_when_stopped.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_stop_fetcher(self, *, app):
        app._fetcher = Mock(stop=AsyncMock())
        await app._stop_fetcher()
        app._fetcher.stop.assert_called_once_with()
        app._fetcher.service_reset.assert_called_once_with()

    def test_on_rebalance_when_stopped(self, *, app):
        app.consumer = Mock()
        app._on_rebalance_when_stopped()
        app.consumer.close.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_partitions_assigned__when_stopped(self, *, app):
        app._stopped.set()
        app._on_rebalance_when_stopped = AsyncMock()
        await app._on_partitions_assigned(set())
        app._on_rebalance_when_stopped.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_partitions_assigned(self, *, app):
        app._assignment = {TP('foo', 1), TP('bar', 2)}
        app.on_partitions_assigned = Mock(send=AsyncMock())
        app.consumer = Mock(
            transactions=Mock(
                on_rebalance=AsyncMock(),
            ),
        )
        app.agents = Mock(
            on_rebalance=AsyncMock(),
        )
        app.tables = Mock(
            on_rebalance=AsyncMock(),
        )
        app.topics = Mock(
            wait_for_subscriptions=AsyncMock(),
            on_partitions_assigned=AsyncMock(),
        )

        assigned = {TP('foo', 1), TP('baz', 3)}
        revoked = {TP('bar', 2)}
        newly_assigned = {TP('baz', 3)}

        app.in_transaction = False
        await app._on_partitions_assigned(assigned)

        app.agents.on_rebalance.assert_called_once_with(
            revoked, newly_assigned)
        app.topics.wait_for_subscriptions.assert_called_once_with()
        app.consumer.pause_partitions.assert_called_once_with(assigned)
        app.topics.on_partitions_assigned.assert_called_once_with(assigned)
        app.consumer.transactions.on_rebalance.assert_not_called()
        app.tables.on_rebalance.assert_called_once_with(
            assigned, revoked, newly_assigned)
        app.on_partitions_assigned.send.assert_called_once_with(assigned)

        app.in_transaction = True
        app._assignment = {TP('foo', 1), TP('bar', 2)}
        await app._on_partitions_assigned(assigned)
        app.consumer.transactions.on_rebalance.assert_called_once_with(
            assigned, revoked, newly_assigned)

    @pytest.mark.asyncio
    async def test_on_partitions_assigned__crashes(self, *, app):
        app._assignment = {TP('foo', 1), TP('bar', 2)}
        app.on_partitions_assigned = Mock(send=AsyncMock())
        app.consumer = Mock()
        app.agents = Mock(
            on_rebalance=AsyncMock(),
        )
        app.agents.on_rebalance.coro.side_effect = RuntimeError()
        app.crash = AsyncMock()

        await app._on_partitions_assigned(set())
        app.crash.assert_called_once()

    @pytest.mark.parametrize('prev,new,expected_revoked,expected_assigned', [
        (None, {TP1, TP2}, set(), {TP1, TP2}),
        (set(), set(), set(), set()),
        (set(), {TP1, TP2}, set(), {TP1, TP2}),
        ({TP1, TP2}, {TP1, TP2}, set(), set()),
        ({TP1, TP2}, {TP1, TP3, TP4}, {TP2}, {TP3, TP4}),
    ])
    def test_update_assignment(
            self, prev, new, expected_revoked, expected_assigned,
            *, app):
        app._assignment = prev
        revoked, newly_assigned = app._update_assignment(new)
        assert revoked == expected_revoked
        assert newly_assigned == expected_assigned
        assert app._assignment == new

    def test_worker_init(self, *, app):
        on_worker_init = app.on_worker_init.connect(
            Mock(name='on_worker_init'))
        fixup1 = Mock(name='fixup1', autospec=Fixup)
        fixup2 = Mock(name='fixup2', autospec=Fixup)
        app.fixups = [fixup1, fixup2]

        app.worker_init()

        fixup1.on_worker_init.assert_called_once_with()
        fixup2.on_worker_init.assert_called_once_with()
        on_worker_init.assert_called_once_with(app, signal=app.on_worker_init)

    def test_discover(self, *, app):
        app.conf.autodiscover = ['a', 'b', 'c']
        app.conf.origin = 'faust'
        fixup1 = Mock(name='fixup1', autospec=Fixup)
        fixup1.autodiscover_modules.return_value = ['d', 'e']
        app.fixups = [fixup1]
        with patch('faust.app.base.venusian'):
            with patch('importlib.import_module') as import_module:
                app.discover()

                import_module.assert_has_calls(
                    [
                        call('a'),
                        call('b'),
                        call('c'),
                        call('d'),
                        call('e'),
                        call('faust'),
                    ],
                    any_order=True,
                )

    def test_discover__disabled(self, *, app):
        app.conf.autodiscover = False
        app.discover()

    def test_discover__unknown_module(self, *, app):
        app.conf.autodiscover = ['xcxz']
        app.conf.origin = 'faust'
        with patch('faust.app.base.venusian'):
            with pytest.raises(ModuleNotFoundError):
                app.discover()

    def test_discovery_modules__bool(self, *, app):
        app.conf.origin = 'faust'
        app.conf.autodiscover = True
        assert app._discovery_modules() == ['faust']

    def test_discovery_modules__callable(self, *, app):
        app.conf.origin = 'faust'
        app.conf.autodiscover = lambda: ['a', 'b', 'c']
        assert app._discovery_modules() == ['a', 'b', 'c', 'faust']

    def test_discovery_modules__list(self, *, app):
        app.conf.origin = 'faust'
        app.conf.autodiscover = ['a', 'b', 'c']
        assert app._discovery_modules() == ['a', 'b', 'c', 'faust']

    def test_discovery_modules__list_no_origin(self, *, app):
        app.conf.origin = None
        app.conf.autodiscover = ['a', 'b', 'c']
        assert app._discovery_modules() == ['a', 'b', 'c']

    def test_discovery_modules__disabled(self, *, app):
        app.conf.origin = 'faust'
        app.conf.autodiscover = False
        assert app._discovery_modules() == []

    def test_discovery_modules__without_origin(self, *, app):
        app.conf.autodiscover = True
        app.conf.origin = None
        with pytest.raises(ImproperlyConfigured):
            app._discovery_modules()

    def test_discover_ignore(self, *, app):
        with patch('faust.app.base.venusian') as venusian:
            app.conf.origin = 'faust'
            app.conf.autodiscover = ['re', 'collections']
            app.discover(categories=['faust.agent'], ignore=['re', 'faust'])

            assert venusian.Scanner().scan.assert_has_calls([
                call(
                    re,
                    categories=('faust.agent', ),
                    ignore=['re', 'faust'],
                ),
                call(
                    faust,
                    categories=('faust.agent', ),
                    ignore=['re', 'faust'],
                ),
                call(
                    collections,
                    categories=('faust.agent', ),
                    ignore=['re', 'faust'],
                ),
            ], any_order=True) is None

    def test_main(self, *, app):
        with patch('faust.cli.faust.cli') as cli:
            app.finalize = Mock(name='app.finalize')
            app.worker_init = Mock(name='app.worker_init')
            app.discover = Mock(name='app.discover')

            app.conf.autodiscover = False
            with pytest.raises(SystemExit):
                app.main()

            z = app.conf.autodiscover
            assert z is False

            app.finalize.assert_called_once_with()
            app.worker_init.assert_called_once_with()
            cli.assert_called_once_with(app=app)

            with pytest.warns(AlreadyConfiguredWarning):
                app.conf.autodiscover = True
            with pytest.raises(SystemExit):
                app.main()

    def test_channel(self, *, app):
        channel = app.channel()
        assert isinstance(channel, Channel)
        assert isinstance(channel, ChannelT)
        assert app.channel(key_type='x').key_type == 'x'
        assert app.channel(value_type='x').value_type == 'x'
        assert app.channel(maxsize=303).maxsize == 303

    def test_agent(self, *, app):
        with patch('faust.app.base.venusian') as venusian:
            @app.agent()
            async def foo(stream):
                ...

            assert foo.name
            assert app.agents[foo.name] is foo

            venusian.attach.assert_called_once_with(foo, category=SCAN_AGENT)

    @pytest.mark.asyncio
    async def test_on_agent_error(self, *, app):
        app._consumer = None
        agent = Mock(name='agent', autospec=Agent)
        await app._on_agent_error(agent, KeyError())

    @pytest.mark.asyncio
    async def test_on_agent_error__consumer(self, *, app):
        app._consumer = Mock(name='consumer', autospec=Consumer)
        agent = Mock(name='agent', autospec=Agent)
        exc = KeyError()
        await app._on_agent_error(agent, exc)
        app._consumer.on_task_error.assert_called_with(exc)

    @pytest.mark.asyncio
    async def test_on_agent_error__MemoryError(self, *, app):
        app._consumer = Mock(name='consumer', autospec=Consumer)
        app._consumer.on_task_error.side_effect = MemoryError()
        agent = Mock(name='agent', autospec=Agent)
        exc = KeyError()
        with pytest.raises(MemoryError):
            await app._on_agent_error(agent, exc)
        app._consumer.on_task_error.assert_called_with(exc)

    def test_task(self, *, app):
        with patch('faust.app.base.venusian') as venusian:
            @app.task
            async def foo():
                ...

            venusian.attach.assert_called_once_with(foo, category=SCAN_TASK)
            assert foo in app._tasks

    @pytest.mark.asyncio
    async def test_task__on_leader(self, *, app):
        @app.task(on_leader=True)
        async def mytask(app):
            return app

        app.is_leader = Mock(return_value=False)
        assert await mytask() is None

        app.is_leader = Mock(return_value=True)
        assert await mytask() is app

    @pytest.mark.asyncio
    async def test_timer(self, *, app):
        did_execute = Mock(name='did_execute')
        app._producer = Mock(name='producer', flush=AsyncMock())

        @app.timer(0.1)
        async def foo():
            did_execute()
            await app.stop()

        await foo()
        did_execute.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_timer__sleep_stopped(self, *, app):
        did_execute = Mock(name='did_execute')
        app.sleep = AsyncMock()

        def on_sleep(seconds):
            app._stopped.set()

        app.sleep.coro.side_effect = on_sleep

        @app.timer(0.1)
        async def foo():
            did_execute()

        await foo()

    @pytest.mark.asyncio
    async def test_timer__on_leader_not_leader(self, *, app):
        did_execute = Mock(name='did_execute')
        app.is_leader = Mock(return_value=False)
        app.sleep = AsyncMock()

        def on_sleep(seconds):
            if app.sleep.call_count >= 3:
                app._stopped.set()
        # cannot use list side_effect arg as it causes
        # StopIteration to be raised.
        app.sleep.coro.side_effect = on_sleep

        @app.timer(300.0, on_leader=True)
        async def foo():
            did_execute()

        await foo()

        assert app.sleep.call_count == 3
        did_execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_timer__on_leader_is_leader(self, *, app):
        did_execute = Mock(name='did_execute')
        app.is_leader = Mock(return_value=True)
        app.sleep = AsyncMock()

        def on_sleep(seconds):
            if app.sleep.call_count >= 3:
                app._stopped.set()
        # cannot use list side_effect arg as it causes
        # StopIteration to be raised.
        app.sleep.coro.side_effect = on_sleep

        @app.timer(300.0, on_leader=True)
        async def foo(app):
            did_execute()

        await foo()

        assert app.sleep.call_count == 3
        assert did_execute.call_count == 2

    @pytest.mark.asyncio
    async def test_crontab(self, *, app):
        did_execute = Mock(name='did_execute')
        app._producer = Mock(name='producer', flush=AsyncMock())

        with patch('faust.app.base.cron') as cron:
            cron.secs_for_next.return_value = 0.1

            @app.crontab('* * * * *')
            async def foo():
                did_execute()
                await app.stop()

            await foo()
            did_execute.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_crontab__on_leader_not_leader(self, *, app):
        did_execute = Mock(name='did_execute')
        with patch('faust.app.base.cron') as cron:
            cron.secs_for_next.return_value = 0.1

            @app.crontab('* * * * *', on_leader=True)
            async def foo():
                did_execute()

        app.is_leader = Mock(return_value=False)
        app.sleep = AsyncMock()

        def on_sleep(seconds):
            if app.sleep.call_count >= 3:
                app._stopped.set()
        app.sleep.coro.side_effect = on_sleep

        await foo()
        assert app.sleep.call_count == 3
        did_execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_crontab__on_leader_is_leader(self, *, app):
        did_execute = Mock(name='did_execute')
        with patch('faust.app.base.cron') as cron:
            cron.secs_for_next.return_value = 0.1

            @app.crontab('* * * * *', on_leader=True)
            async def foo(app):
                did_execute()

        app.is_leader = Mock(return_value=True)
        app.sleep = AsyncMock()

        def on_sleep(seconds):
            if app.sleep.call_count >= 3:
                app._stopped.set()
        app.sleep.coro.side_effect = on_sleep

        await foo()
        assert app.sleep.call_count == 3
        assert did_execute.call_count == 2

    def test_service(self, *, app):

        @app.service
        class Foo(Service):
            ...

        assert Foo in app._extra_services

    def test_is_leader(self, *, app):
        app._leader_assignor = Mock(
            name='_leader_assignor',
            autospec=LeaderAssignor,
        )
        app._leader_assignor.is_leader.return_value = True
        assert app.is_leader()

    def test_Table(self, *, app):
        table = app.Table('name')
        assert app.tables.data['name'] is table

    def test_SetTable(self, *, app):
        table = app.SetTable('name')
        assert app.tables.data['name'] is table

    def test_page(self, *, app):

        with patch('faust.app.base.venusian') as venusian:

            @app.page('/foo')
            async def view(self, request):
                ...

            assert '/foo' in app.web.views

            venusian.attach.assert_called_once_with(view, category=SCAN_PAGE)

    def test_page__with_cors_options(self, *, app):

        with patch('faust.app.base.venusian') as venusian:

            @app.page(
                '/foo',
                cors_options={
                    'http://foo.example.com': ResourceOptions(
                        allow_credentials=True,
                        expose_headers='*',
                        allow_headers='*',
                        max_age=None,
                        allow_methods='*',
                    ),
                },
            )
            async def view(self, request):
                ...

            assert '/foo' in app.web.views

            venusian.attach.assert_called_once_with(view, category=SCAN_PAGE)

    def test_page__view_class_but_not_view(self, *, app):
        with pytest.raises(TypeError):
            @app.page('/foo')
            class Foo:
                ...

    @pytest.mark.asyncio
    async def test_table_route__query_param(self, *, app):
        table = app.Table('foo')
        view = Mock()
        request = Mock()
        app.router.route_req = AsyncMock()

        @app.table_route(table, query_param='q')
        async def routed(self, request):
            return 42

        request.query = {'q': 'KEY'}

        ret = await routed(view, request)
        assert ret is app.router.route_req.coro.return_value

        app.router.route_req.coro.side_effect = SameNode()
        ret = await routed(view, request)
        assert ret == 42

    @pytest.mark.asyncio
    async def test_table_route__match_info(self, *, app):
        table = app.Table('foo')
        view = Mock()
        request = Mock()
        app.router.route_req = AsyncMock()

        @app.table_route(table, match_info='q')
        async def routed(self, request):
            return 42

        request.match_info = {'q': 'KEY'}

        ret = await routed(view, request)
        assert ret is app.router.route_req.coro.return_value

        app.router.route_req.coro.side_effect = SameNode()
        ret = await routed(view, request)
        assert ret == 42

    def test_table_route__compat_shard_param(self, *, app):
        table = app.Table('foo')
        with pytest.warns(DeprecationWarning):
            @app.table_route(table, shard_param='x')
            async def view(self, request):
                ...

    def test_table_route__query_param_and_shard_param(self, *, app):
        table = app.Table('foo')
        with pytest.warns(DeprecationWarning):
            with pytest.raises(TypeError):
                @app.table_route(table, query_param='q', shard_param='x')
                async def view(self, request):
                    ...

    def test_table_route__missing_param(self, *, app):
        table = app.Table('foo')
        with pytest.raises(TypeError):
            @app.table_route(table)
            async def view(self, request):
                ...

    def test_command(self, *, app):
        @app.command()
        async def foo():
            ...

    def test_command__with_base(self, *, app):
        class MyBase(AppCommand):
            ...

        @app.command(base=MyBase)
        async def foo():
            ...

        assert issubclass(foo, MyBase)

    @pytest.mark.asyncio
    async def test_start_client(self, *, app):
        app.maybe_start = AsyncMock(name='app.maybe_start')
        await app.start_client()
        assert app.client_only
        app.maybe_start.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_maybe_start_client(self, *, app):
        app.start_client = AsyncMock(name='start_client')
        app._started.set()
        await app.maybe_start_client()
        app.start_client.assert_not_called()

        app._started.clear()
        await app.maybe_start_client()
        app.start_client.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_commit(self, *, app):
        app.topics = Mock(
            name='topics',
            autospec=Conductor,
            commit=AsyncMock(),
        )
        await app.commit({1})
        app.topics.commit.assert_called_with({1})

    def test_Worker(self, *, app):
        app.conf = Mock(name='conf', autospec=Settings)
        worker = app.Worker(loglevel=10)
        app.conf.Worker.assert_called_once_with(app, loglevel=10)
        assert worker is app.conf.Worker()

    def test_create_directories(self, *, app):
        app.conf = Mock(name='conf', autospec=Settings)

        app._create_directories()

        app.conf.datadir.mkdir.assert_called_once_with(exist_ok=True)
        app.conf.appdir.mkdir.assert_called_once_with(exist_ok=True)
        app.conf.tabledir.mkdir.assert_called_once_with(exist_ok=True)

    @pytest.mark.asyncio
    async def test_maybe_start_producer(self, *, app):
        app.in_transaction = True
        assert await app.maybe_start_producer() is app.consumer.transactions

        app.in_transaction = False
        app.producer = Mock(maybe_start=AsyncMock())
        assert await app.maybe_start_producer() is app.producer
        app.producer.maybe_start.coro.assert_called_once_with()

    def test_repr(self, *, app):
        assert repr(app)

    def test_repr__unfinialized(self, *, app):
        app._conf = None
        assert repr(app)

    def test_monitor(self, *, app):
        assert app._monitor is None
        app.conf.Monitor = Mock(
            name='Monitor',
            return_value=Mock(autospec=Monitor),
        )
        monitor = app.monitor
        app.conf.Monitor.assert_called_once_with(
            loop=app.loop, beacon=app.beacon)
        assert monitor is app.conf.Monitor()
        assert app.monitor is monitor
        assert app._monitor is monitor

        monitor2 = app.monitor = Mock(
            name='monitor2',
            autospec=Monitor,
        )
        assert app._monitor is monitor2
        assert app.monitor is monitor2

    def test_fetcher(self, *, app):
        app.transport.Fetcher = Mock(
            name='Fetcher',
            return_value=Mock(autospec=Fetcher),
        )
        fetcher = app._fetcher
        app.transport.Fetcher.assert_called_once_with(
            app, loop=app.loop, beacon=app.beacon,
        )
        assert fetcher is app.transport.Fetcher()

    def test_reply_consumer(self, *, app):
        with patch('faust.app.base.ReplyConsumer') as ReplyConsumer:
            reply_consumer = app._reply_consumer
            ReplyConsumer.assert_called_once_with(
                app, loop=app.loop, beacon=app.beacon)
            assert reply_consumer is ReplyConsumer()

    def test_label(self, *, app):
        assert app.label

    def test_cache(self, *, app):
        assert app.cache
        assert app.cache is app._cache
        obj = object()
        app.cache = obj
        assert app.cache is obj

    def test_http_client(self, *, app):
        app.conf.HttpClient = Mock(name='HttpClient')
        assert app._http_client is None
        client = app.http_client
        app.conf.HttpClient.assert_called_once_with()
        assert client is app.conf.HttpClient()
        assert app._http_client is client
        assert app.http_client is client

        obj = object()
        app.http_client = obj
        assert app.http_client is obj

    def test_leader_assignor(self, *, app):
        leader_assignor = app._leader_assignor
        assert isinstance(leader_assignor, LeaderAssignorT)
        assert isinstance(leader_assignor, LeaderAssignor)
        assert leader_assignor.app is app
        assert leader_assignor.beacon.parent is app.beacon


class test_AppConfiguration:

    def test_conf__before_finalized(self, *, monkeypatch, app):
        app.finalized = False
        monkeypatch.setattr('faust.app.base.STRICT', False)
        app.conf.id
        monkeypatch.setattr('faust.app.base.STRICT', True)
        with pytest.raises(ImproperlyConfigured):
            app.conf.id

    def test_set_conf(self, *, app):
        conf = Mock(name='conf', autospec=Settings)
        app.conf = conf
        assert app.conf is conf

    @pytest.mark.parametrize('config_source', [
        ConfigClass,
        CONFIG_DICT,
        CONFIG_PATH,
    ])
    def test_config_From_object(self, config_source, *, app):
        on_before = app.on_before_configured.connect(Mock(name='on_before'))
        on_config = app.on_configured.connect(Mock(name='on_config'))
        on_after = app.on_after_configured.connect(Mock(name='on_after'))

        app.configured = False
        app.finalized = False
        app.config_from_object(config_source)
        app._conf = None
        app.finalized = True

        app.configured = False
        with pytest.warns(AlreadyConfiguredWarning):
            app.config_from_object(config_source)

        assert app._config_source is config_source
        app._config_source = None

        app.configured = True
        with pytest.warns(AlreadyConfiguredWarning):
            app.config_from_object(config_source)

        assert app._config_source is config_source
        on_before.assert_called_with(app, signal=app.on_before_configured)
        on_config.assert_called_with(app, app.conf, signal=app.on_configured)
        on_after.assert_called_with(app, signal=app.on_after_configured)
        assert app.conf.broker == [URL('kafka://foo')]
        assert app.conf.stream_buffer_maxsize == 1

    def test_finalize__no_id(self, *, app):
        with pytest.warns(AlreadyConfiguredWarning):
            app.conf.id = None
        app.finalized = False
        with pytest.raises(ImproperlyConfigured):
            app.finalize()

    def test_load_settings_from_source__no_attribute(self, *, app):
        with pytest.raises(AttributeError):
            app._load_settings_from_source('faust.app.base.NOTEXIST')
        assert app._load_settings_from_source(
            'faust.app.base.NOTEXIST', silent=True) == {}

    def test_load_settings_from_source__import_error(self, *, app):
        with pytest.raises(ImportError):
            app._load_settings_from_source('zxzc')
        assert app._load_settings_from_source('zxzc', silent=True) == {}

    def test_load_settings_with_compat_and_new_settings(self, *, app):
        config = {
            'client_id': 'foo',
            'broker_client_id': 'bar',
        }
        with pytest.warns(AlreadyConfiguredWarning):
            with pytest.raises(ImproperlyConfigured):
                app.config_from_object(config)

    @pytest.mark.asyncio
    async def test_wait_for_table_recovery__producer_only(self, *, app):
        app.producer_only = True
        assert await app._wait_for_table_recovery_completed() is False

    @pytest.mark.asyncio
    async def test_wait_for_table_recovery__client_only(self, *, app):
        app.client_only = True
        assert await app._wait_for_table_recovery_completed() is False
