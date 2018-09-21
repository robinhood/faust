import re
import faust
from faust.agents import Agent
from faust.agents.manager import AgentManager
from faust.app.base import SCAN_AGENT, SCAN_PAGE, SCAN_TASK
from faust.app.service import AppService
from faust.assignor.leader_assignor import LeaderAssignor, LeaderAssignorT
from faust.channels import Channel, ChannelT
from faust.exceptions import ImproperlyConfigured
from faust.fixups.base import Fixup
from faust.sensors.monitor import Monitor
from faust.serializers import codecs
from faust.tables.manager import TableManager
from faust.transport.base import Transport
from faust.transport.conductor import Conductor
from faust.transport.consumer import Consumer, Fetcher
from faust.types.models import ModelT
from faust.types.settings import Settings
from mode import Service
from mode.utils.futures import FlowControlEvent
from mode.utils.compat import want_bytes
from mode.utils.logging import CompositeLogger
from mode.utils.mocks import ANY, AsyncMock, MagicMock, Mock, call, patch
from yarl import URL
import pytest

TEST_TOPIC = 'test'
CONFIG_DICT = {
    'broker': 'kafka://foo',
    'stream_buffer_maxsize': 1,
}
CONFIG_PATH = 't.unit.app.test_base.ConfigClass'


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
        expected_topic, expected_key, event.dumps(), partition=None,
    )


@pytest.mark.asyncio
async def test_send_str(app):
    await app.send('foo', Value(amount=0.0))


@pytest.mark.asyncio
@pytest.mark.parametrize('revoked,assignment', [
    ({1, 2, 3, 7, 9, 101, 1001}, None),
    ({1, 2, 3, 7, 9, 101, 1001}, {}),
    ({1, 2, 3}, None),
    ({1, 2}, None),
    ({1, 2}, {}),
    ({1}, None),
    ({1}, {}),
    ({}, None),
    ({}, {}),
])
async def test_on_partitions_revoked(revoked, assignment, *, app):
    if assignment is None:
        assignment = revoked
    app.topics = MagicMock(
        name='app.topics',
        autospec=Conductor,
        on_partitions_revoked=AsyncMock(),
    )
    app.tables = Mock(
        name='app.tables',
        autospec=TableManager,
        on_partitions_revoked=AsyncMock(),
        _stop_standbys=AsyncMock(),  # XXX should not use internal method
    )
    app._fetcher = Mock(
        name='app._fetcher',
        autospec=Fetcher,
        stop=AsyncMock(),
    )
    app.consumer = Mock(
        name='app.consumer',
        autospec=Consumer,
        pause_partitions=AsyncMock(),
        wait_empty=AsyncMock(),
    )
    app.flow_control = Mock(
        name='app.flow_control',
        autospec=FlowControlEvent,
    )
    app.agents = Mock(
        name='app.agents',
        autospec=AgentManager,
    )
    signal = app.on_partitions_revoked.connect(AsyncMock(name='signal'))

    app.consumer.assignment.return_value = None
    app.conf.stream_wait_empty = False
    await app._on_partitions_revoked(revoked)

    app.topics.on_partitions_revoked.assert_called_once_with(revoked)
    app.tables.on_partitions_revoked.assert_called_once_with(revoked)
    app._fetcher.stop.assert_called_once_with()
    signal.assert_called_with(app, revoked, signal=app.on_partitions_revoked)

    assignment = app.consumer.assignment.return_value = revoked
    await app._on_partitions_revoked(revoked)

    if assignment:
        app.flow_control.suspend.assert_called_once_with()
        app.consumer.pause_partitions.assert_called_once_with(assignment)
        app.flow_control.clear.assert_called_once_with()

        app.conf.stream_wait_empty = True
        await app._on_partitions_revoked(revoked)
        app.consumer.wait_empty.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize('assigned', [
    {1, 2, 3, 7, 9, 101, 1001},
    {1, 2, 3},
    {1, 2},
    {1},
    {},
])
async def test_on_partitions_assigned(assigned, *, app):
    app.consumer = Mock(
        name='app.consumer',
        autospec=Consumer,
        pause_partitions=AsyncMock(),
    )
    app.agents = Mock(
        name='app.agents',
        autospec=AgentManager,
        on_partitions_assigned=AsyncMock(),
    )
    app.topics = MagicMock(
        name='app.topics',
        autospec=Conductor,
        on_partitions_assigned=AsyncMock(),
        wait_for_subscriptions=AsyncMock(),
    )
    app.tables = Mock(
        name='app.tables',
        autospec=TableManager,
        on_partitions_assigned=AsyncMock(),
    )
    app._fetcher = Mock(
        name='app._fetcher',
        autospec=Fetcher,
        restart=AsyncMock(),
        stop=AsyncMock(),
    )
    app.flow_control = Mock(
        name='app.flow_control',
        autospec=FlowControlEvent,
    )
    signal = app.on_partitions_assigned.connect(AsyncMock(name='signal'))

    await app._on_partitions_assigned(assigned)

    app.agents.on_partitions_assigned.assert_called_once_with(assigned)
    app.topics.wait_for_subscriptions.assert_called_once_with()
    app.consumer.pause_partitions.assert_called_once_with(assigned)
    app.topics.on_partitions_assigned.assert_called_once_with(assigned)
    app.tables.on_partitions_assigned.assert_called_once_with(assigned)
    app.flow_control.resume.assert_called_once_with()
    signal.assert_called_once_with(
        app, assigned, signal=app.on_partitions_assigned)

    app.log = Mock(name='log', autospec=CompositeLogger)
    await app._on_partitions_assigned(assigned)


class test_App:

    def test_stream(self, *, app):
        s = app.topic(TEST_TOPIC).stream()
        assert s.channel.topics == (TEST_TOPIC,)
        assert s.channel in app.topics
        assert s.channel.app == app

    def test_new_producer(self, *, app):
        del(app.producer)
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
        by_url.assert_called_with(app.conf.broker)
        by_url.return_value.assert_called_with(
            app.conf.broker, app, loop=app.loop)
        app.transport = 10
        assert app.transport == 10

    @pytest.mark.asyncio
    async def test_on_stop(self, *, app):
        app._http_client = Mock(name='http_client', close=AsyncMock())
        await app.on_stop()
        app._http_client.close.assert_called_once_with()
        app._http_client = None
        await app.on_stop()

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

    def test_discovery_modules__disabled(self, *, app):
        app.conf.origin = 'faust'
        app.conf.autodiscover = False
        assert app._discovery_modules() == []

    def test_discovery_modules__without_origin(self, *, app):
        app.conf.autodiscover = True
        app.conf.origin = None
        with pytest.raises(ImproperlyConfigured):
            app._discovery_modules()

    def test_new_scanner(self, *, app):
        with patch('faust.app.base.venusian') as venusian:
            pat = re.compile('^foo')
            scanner = app._new_scanner(pat)
            venusian.Scanner.assert_called_with(ignore=[pat.search])
            assert scanner is venusian.Scanner()

    def test_main(self, *, app):
        with patch('faust.cli.faust.cli') as cli:
            app.finalize = Mock(name='app.finalize')
            app.worker_init = Mock(name='app.worker_init')
            app.discover = Mock(name='app.discover')

            app.conf.autodiscover = False
            app.main()

            app.finalize.assert_called_once_with()
            app.worker_init.assert_called_once_with()
            cli.assert_called_once_with(app=app)

            app.conf.autodiscover = True
            app.main()
            app.discover.assert_called_once_with()

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
    async def test_timer(self, *, app):
        did_execute = Mock(name='did_execute')

        @app.timer(0.1)
        async def foo():
            did_execute()
            await app.stop()

        await foo()
        did_execute.assert_called_once_with()

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

    def test_page(self, *, app):

        with patch('faust.app.base.venusian') as venusian:

            @app.page('/foo')
            async def view(self, request):
                ...

            assert ('', view) in app.pages

            venusian.attach.assert_called_once_with(view, category=SCAN_PAGE)

    def test_table_route(self, *, app):
        table = app.Table('foo')

        @app.table_route(table, query_param='q')
        async def view(self, request):
            ...

    def test_command(self, *, app):
        @app.command()
        async def foo():
            ...

    @pytest.mark.asyncio
    async def test_start_client(self, *, app):
        app._service = Mock(
            name='_service',
            autospec=AppService,
            maybe_start=AsyncMock(),
        )
        await app.start_client()
        assert app.client_only
        app._service.maybe_start.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_maybe_start_client(self, *, app):
        app.start_client = AsyncMock(name='start_client')
        app._service = Mock(
            name='_service',
            autospec=AppService,
        )

        app._service.started = True
        await app.maybe_start_client()
        app.start_client.assert_not_called()

        app._service.started = False
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

    def test_repr(self, *, app):
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

    def test_http_client(self, *, app):
        app.conf.HttpClient = Mock(name='HttpClient')
        assert app._http_client is None
        client = app.http_client
        app.conf.HttpClient.assert_called_once_with()
        assert client is app.conf.HttpClient()
        assert app._http_client is client
        assert app.http_client is client

    def test_leader_assignor(self, *, app):
        leader_assignor = app._leader_assignor
        assert isinstance(leader_assignor, LeaderAssignorT)
        assert isinstance(leader_assignor, LeaderAssignor)
        assert leader_assignor.app is app
        assert leader_assignor.beacon.parent is app.beacon


class test_AppConfiguration:

    def test_conf__before_finalized(self, *, app):
        app.finalized = False
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
        app.config_from_object(config_source)

        assert app._config_source is config_source
        app._config_source = None

        app.configured = True
        app.config_from_object(config_source)

        assert app._config_source is config_source
        on_before.assert_called_with(app, signal=app.on_before_configured)
        on_config.assert_called_with(app, app.conf, signal=app.on_configured)
        on_after.assert_called_with(app, signal=app.on_after_configured)
        assert app.conf.broker == URL('kafka://foo')
        assert app.conf.stream_buffer_maxsize == 1

    def test_finalize__no_id(self, *, app):
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
        with pytest.raises(ImproperlyConfigured):
            app.config_from_object(config)
