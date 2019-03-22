import socket
import sys
from pathlib import Path

import faust
import mode
import pytest
import pytz
from aiohttp.client import ClientSession
from faust import App
from faust.app import BootStrategy
from faust.assignor import LeaderAssignor, PartitionAssignor
from faust.exceptions import AlreadyConfiguredWarning, ImproperlyConfigured
from faust.app.router import Router
from faust.sensors import Monitor
from faust.serializers import Registry
from faust.tables import TableManager
from faust.transport.utils import DefaultSchedulingStrategy
from faust.types import settings
from faust.types.enums import ProcessingGuarantee
from faust.types.web import ResourceOptions
from yarl import URL

TABLEDIR: Path
DATADIR: Path
if sys.platform == 'win32':
    TABLEDIR = Path('c:/Program Files/Faust/')
    DATADIR = Path('c:/Temporary Files/Faust/')
else:
    DATADIR = Path('/etc/faust/')
    TABLEDIR = Path('/var/faust/')


class OtherSchedulingStrategy(DefaultSchedulingStrategy):
    ...


def _dummy_partitioner(a, b, c):
    return 0


class test_settings:

    def App(self, id='myid', **kwargs):
        app = App(id, **kwargs)
        app.finalize()
        return app

    def test_defaults(self):
        app = self.App()
        conf = app.conf
        assert conf.broker == [URL(settings.BROKER_URL)]
        assert conf.store == URL(settings.STORE_URL)
        assert conf.cache == URL(settings.CACHE_URL)
        assert conf.web == URL(settings.WEB_URL)
        assert conf.web_enabled
        assert not conf.web_in_thread
        assert conf.datadir == conf._prepare_datadir(settings.DATADIR)
        assert conf.tabledir == conf._prepare_tabledir(settings.TABLEDIR)
        assert conf.processing_guarantee == settings.PROCESSING_GUARANTEE
        assert conf.broker_client_id == settings.BROKER_CLIENT_ID
        assert conf.broker_request_timeout == settings.BROKER_REQUEST_TIMEOUT
        assert conf.broker_session_timeout == settings.BROKER_SESSION_TIMEOUT
        assert (conf.broker_heartbeat_interval ==
                settings.BROKER_HEARTBEAT_INTERVAL)
        assert conf.broker_commit_interval == settings.BROKER_COMMIT_INTERVAL
        assert conf.broker_commit_every == settings.BROKER_COMMIT_EVERY
        assert (conf.broker_commit_livelock_soft_timeout ==
                settings.BROKER_LIVELOCK_SOFT)
        assert conf.broker_check_crcs
        assert conf.timezone is settings.TIMEZONE
        assert conf.table_cleanup_interval == settings.TABLE_CLEANUP_INTERVAL
        assert conf.reply_to_prefix == settings.REPLY_TO_PREFIX
        assert conf.reply_expires == settings.REPLY_EXPIRES
        assert conf.stream_buffer_maxsize == settings.STREAM_BUFFER_MAXSIZE
        assert conf.stream_recovery_delay == settings.STREAM_RECOVERY_DELAY
        assert conf.producer_partitioner is None
        assert (conf.producer_request_timeout ==
                settings.PRODUCER_REQUEST_TIMEOUT)
        assert (conf.stream_publish_on_commit ==
                settings.STREAM_PUBLISH_ON_COMMIT)
        assert conf.stream_wait_empty
        assert not conf.stream_ack_cancelled_tasks
        assert conf.stream_ack_exceptions
        assert (conf.broker_max_poll_records ==
                settings.BROKER_MAX_POLL_RECORDS)
        assert (conf.consumer_auto_offset_reset ==
                settings.CONSUMER_AUTO_OFFSET_RESET)
        assert not conf.autodiscover
        assert conf.origin is None
        assert conf.key_serializer == 'raw'
        assert conf.value_serializer == 'json'
        assert conf.reply_to is not None
        assert not conf.reply_create_topic
        assert conf.table_standby_replicas == 1
        assert conf.topic_replication_factor == 1
        assert conf.topic_partitions == 8
        assert conf.topic_allow_declare
        assert conf.logging_config is None
        assert conf.loghandlers == []
        assert conf.version == 1
        assert conf.canonical_url == URL(f'http://{socket.gethostname()}:6066')
        assert conf.web_bind == '0.0.0.0'
        assert conf.web_port == 6066
        assert conf.web_transport == settings.WEB_TRANSPORT
        assert conf.web_cors_options is None
        assert conf.worker_redirect_stdouts
        assert conf.worker_redirect_stdouts_level == 'WARN'

        assert conf.agent_supervisor is mode.OneForOneSupervisor

        assert conf.Agent is faust.Agent
        assert conf.ConsumerScheduler is DefaultSchedulingStrategy
        assert conf.Stream is faust.Stream
        assert conf.Table is faust.Table
        assert conf.TableManager is TableManager
        assert conf.Serializers is Registry
        assert conf.Worker is faust.Worker
        assert conf.PartitionAssignor is PartitionAssignor
        assert conf.LeaderAssignor is LeaderAssignor
        assert conf.Router is Router
        assert conf.Topic is faust.Topic
        assert conf.HttpClient is ClientSession
        assert conf.Monitor is Monitor

    def test_reply_prefix_unique(self):
        app1 = self.App()
        app2 = self.App()
        assert app1.conf.reply_to != app2.conf.reply_to

    def test_app_config(self):
        self.assert_config_equivalent()

    def test_broker_as_URL(self):
        app = self.assert_config_equivalent(broker='ckafka://')
        assert isinstance(app.conf.broker, list)
        assert app.conf.broker[0] == URL('ckafka://')

    def test_store_as_URL(self):
        app = self.assert_config_equivalent(store=URL('moo://'))
        assert isinstance(app.conf.store, URL)

    def test_cache_as_URL(self):
        app = self.assert_config_equivalent(cache=URL('moo://'))
        assert isinstance(app.conf.cache, URL)

    def test_web_as_URL(self):
        app = self.assert_config_equivalent(web=URL('moo://'))
        assert isinstance(app.conf.web, URL)

    def test_datadir_as_Path(self):
        app = self.assert_config_equivalent(datadir=DATADIR)
        assert isinstance(app.conf.datadir, Path)

    def test_tabledir_is_relative_to_path(self):
        app = self.assert_config_equivalent(
            datadir=str(DATADIR),
            tabledir='moo',
        )
        assert app.conf.tabledir == app.conf.appdir / Path('moo')

    def assert_config_equivalent(self,
                                 id='id',
                                 version=303,
                                 broker='foo://',
                                 store='bar://',
                                 cache='baz://',
                                 web='xuzzy://',
                                 web_enabled=False,
                                 autodiscover=True,
                                 origin='faust',
                                 canonical_url='http://example.com/',
                                 broker_client_id='client id',
                                 datadir=str(DATADIR),
                                 tabledir=str(TABLEDIR),
                                 processing_guarantee='exactly_once',
                                 broker_request_timeout=10000.05,
                                 broker_heartbeat_interval=101.13,
                                 broker_session_timeout=30303.30,
                                 broker_commit_every=202,
                                 broker_commit_interval=30.3,
                                 broker_commit_livelock_soft_timeout=60.6,
                                 broker_check_crcs=False,
                                 producer_partitioner=_dummy_partitioner,
                                 producer_request_timeout=2.66,
                                 table_cleanup_interval=80.8,
                                 key_serializer='str',
                                 value_serializer='str',
                                 table_standby_replicas=48,
                                 topic_replication_factor=16,
                                 topic_allow_declare=False,
                                 reply_to='reply_to',
                                 reply_create_topic=True,
                                 reply_expires=90.9,
                                 stream_buffer_maxsize=101,
                                 stream_wait_empty=True,
                                 stream_ack_cancelled_tasks=True,
                                 stream_ack_exceptions=False,
                                 stream_publish_on_commit=False,
                                 stream_recovery_delay=69.3,
                                 web_bind='localhost',
                                 web_port=6069,
                                 web_host='localhost',
                                 web_transport='udp://',
                                 web_in_thread=True,
                                 web_cors_options={  # noqa: B006
                                    'http://example.com': ResourceOptions(
                                        allow_credentials=True,
                                        expose_headers='*',
                                        allow_headers='*',
                                        max_age=3132,
                                        allow_methods='*',
                                    ),
                                 },
                                 worker_redirect_stdouts=False,
                                 worker_redirect_stdouts_level='DEBUG',
                                 broker_max_poll_records=1000,
                                 timezone=pytz.timezone('US/Eastern'),
                                 logging_config={'foo': 10},  # noqa
                                 consumer_auto_offset_reset='latest',
                                 ConsumerScheduler=OtherSchedulingStrategy,
                                 **kwargs) -> App:
        livelock_soft_timeout = broker_commit_livelock_soft_timeout
        app = self.App(
            id,
            version=version,
            broker=broker,
            store=store,
            cache=cache,
            web=web,
            web_enabled=web_enabled,
            autodiscover=autodiscover,
            origin=origin,
            canonical_url=canonical_url,
            broker_client_id=broker_client_id,
            datadir=datadir,
            tabledir=tabledir,
            processing_guarantee=processing_guarantee,
            broker_request_timeout=broker_request_timeout,
            broker_session_timeout=broker_session_timeout,
            broker_heartbeat_interval=broker_heartbeat_interval,
            broker_commit_every=broker_commit_every,
            broker_commit_interval=broker_commit_interval,
            broker_commit_livelock_soft_timeout=livelock_soft_timeout,
            broker_check_crcs=broker_check_crcs,
            broker_max_poll_records=broker_max_poll_records,
            producer_partitioner=producer_partitioner,
            producer_request_timeout=producer_request_timeout,
            table_cleanup_interval=table_cleanup_interval,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            table_standby_replicas=table_standby_replicas,
            topic_replication_factor=topic_replication_factor,
            topic_allow_declare=topic_allow_declare,
            reply_to=reply_to,
            reply_create_topic=reply_create_topic,
            reply_expires=reply_expires,
            stream_buffer_maxsize=stream_buffer_maxsize,
            stream_wait_empty=stream_wait_empty,
            stream_ack_cancelled_tasks=stream_ack_cancelled_tasks,
            stream_ack_exceptions=stream_ack_exceptions,
            stream_publish_on_commit=stream_publish_on_commit,
            stream_recovery_delay=stream_recovery_delay,
            timezone=timezone,
            web_bind=web_bind,
            web_port=web_port,
            web_host=web_host,
            web_transport=web_transport,
            web_in_thread=web_in_thread,
            web_cors_options=web_cors_options,
            worker_redirect_stdouts=worker_redirect_stdouts,
            worker_redirect_stdouts_level=worker_redirect_stdouts_level,
            logging_config=logging_config,
            consumer_auto_offset_reset=consumer_auto_offset_reset,
            ConsumerScheduler=ConsumerScheduler,
        )
        conf = app.conf
        assert conf.id == app.conf._prepare_id(id)
        assert conf.broker == [URL(broker)]
        assert conf.store == URL(str(store))
        assert conf.cache == URL(str(cache))
        assert conf.web == URL(str(web))
        assert not conf.web_enabled
        assert conf.web_in_thread
        assert conf.autodiscover == autodiscover
        assert conf.canonical_url == URL(str(canonical_url))
        assert conf.broker_client_id == broker_client_id
        assert conf.datadir == Path(str(datadir))
        if Path(tabledir).is_absolute():
            assert conf.tabledir == Path(str(tabledir))
        else:
            assert conf.tabledir.relative_to(conf.appdir) == Path(tabledir)
        assert conf.processing_guarantee == ProcessingGuarantee.EXACTLY_ONCE
        assert conf.broker_request_timeout == broker_request_timeout
        assert conf.broker_heartbeat_interval == broker_heartbeat_interval
        assert conf.broker_session_timeout == broker_session_timeout
        assert conf.broker_commit_every == broker_commit_every
        assert conf.broker_commit_interval == broker_commit_interval
        assert (conf.broker_commit_livelock_soft_timeout ==
                broker_commit_livelock_soft_timeout)
        assert conf.broker_check_crcs == broker_check_crcs
        assert conf.producer_partitioner is producer_partitioner
        assert conf.producer_request_timeout == producer_request_timeout
        assert conf.table_cleanup_interval == table_cleanup_interval
        assert conf.key_serializer == key_serializer
        assert conf.value_serializer == value_serializer
        assert conf.table_standby_replicas == table_standby_replicas
        assert conf.topic_replication_factor == topic_replication_factor
        assert conf.topic_allow_declare == topic_allow_declare
        assert conf.reply_to == reply_to
        assert conf.reply_expires == reply_expires
        assert conf.stream_buffer_maxsize == stream_buffer_maxsize
        assert conf.stream_wait_empty == stream_wait_empty
        assert conf.stream_ack_cancelled_tasks == stream_ack_cancelled_tasks
        assert conf.stream_ack_exceptions == stream_ack_exceptions
        assert conf.stream_publish_on_commit == stream_publish_on_commit
        assert conf.stream_recovery_delay == stream_recovery_delay
        assert conf.timezone is timezone
        assert conf.web_bind == web_bind
        assert conf.web_port == web_port
        assert conf.web_host == web_host
        assert conf.web_transport == URL(web_transport)
        assert conf.web_cors_options == web_cors_options
        assert conf.worker_redirect_stdouts == worker_redirect_stdouts
        assert (conf.worker_redirect_stdouts_level ==
                worker_redirect_stdouts_level)
        assert conf.broker_max_poll_records == broker_max_poll_records
        assert conf.logging_config == logging_config
        assert conf.consumer_auto_offset_reset == consumer_auto_offset_reset
        assert conf.ConsumerScheduler is OtherSchedulingStrategy
        return app

    def test_custom_host_port_to_canonical(self,
                                           web_bind='localhost',
                                           web_port=6069,
                                           web_host='localhost'):
        app = self.App(
            'id',
            web_bind=web_bind,
            web_port=web_port,
            web_host=web_host,
        )
        assert app.conf.canonical_url == URL(
            f'http://{app.conf.web_host}:{app.conf.web_port}')

    def test_id_no_version(self):
        assert self.App('id', version=1).conf.id == 'id'

    def test_version_cannot_be_zero(self):
        app = App('id', version=0)
        with pytest.raises(ImproperlyConfigured):
            app.finalize()

    def test_compat_url(self):
        assert self.App(url='foo').conf.broker == [URL('kafka://foo')]

    def test_compat_client_id(self):
        with pytest.warns(FutureWarning):
            assert self.App(client_id='foo').conf.broker_client_id == 'foo'

    def test_compat_commit_interval(self):
        with pytest.warns(FutureWarning):
            assert self.App(
                commit_interval=313.3).conf.broker_commit_interval == 313.3

    def test_compat_create_reply_topic(self):
        with pytest.warns(FutureWarning):
            assert self.App(create_reply_topic=True).conf.reply_create_topic
        with pytest.warns(FutureWarning):
            assert not self.App(
                create_reply_topic=False).conf.reply_create_topic

    def test_compat_num_standby_replicas(self):
        with pytest.warns(FutureWarning):
            assert self.App(
                num_standby_replicas=34).conf.table_standby_replicas == 34

    def test_compat_default_partitions(self):
        with pytest.warns(FutureWarning):
            assert self.App(
                default_partitions=35).conf.topic_partitions == 35

    def test_compat_replication_factor(self):
        with pytest.warns(FutureWarning):
            assert self.App(
                replication_factor=36).conf.topic_replication_factor == 36

    def test_warns_when_key_already_configured(self):
        app = self.App(topic_partitions=37, topic_replication_factor=38)
        assert app.conf.topic_partitions == 37
        with pytest.warns(AlreadyConfiguredWarning):
            app.conf.topic_partitions = 39
        assert app.conf.topic_partitions == 39
        app.conf.topic_replication_factor = 40
        assert app.conf.topic_replication_factor == 40

    def test_broker_with_no_scheme_set(self):
        app = self.App(broker='example.com:3123')
        url = app.conf.broker[0]
        assert url.scheme == settings.DEFAULT_BROKER_SCHEME
        assert url.host == 'example.com'
        assert url.port == 3123


class test_BootStrategy:

    def test_init(self, *, app):
        assert not BootStrategy(app, enable_web=False).enable_web
        assert BootStrategy(app, enable_web=True).enable_web
        assert not BootStrategy(app, enable_kafka=False).enable_kafka
        assert BootStrategy(app, enable_kafka=True).enable_kafka
        assert not BootStrategy(
            app, enable_kafka_producer=False,
        ).enable_kafka_producer
        assert BootStrategy(
            app, enable_kafka_producer=True,
        ).enable_kafka_producer
        assert not BootStrategy(
            app, enable_kafka_consumer=False,
        ).enable_kafka_consumer
        assert BootStrategy(
            app, enable_kafka_consumer=True,
        ).enable_kafka_consumer
        assert not BootStrategy(app, enable_sensors=False).enable_sensors
        assert BootStrategy(app, enable_sensors=True).enable_sensors

    def test_sensors(self, *, app):
        assert BootStrategy(app, enable_sensors=True).sensors() is app.sensors
        assert not BootStrategy(app, enable_sensors=False).sensors()

    def test_kafka_consumer(self, *, app):
        assert BootStrategy(
            app, enable_kafka_consumer=True).kafka_consumer()
        assert BootStrategy(
            app, enable_kafka_consumer=True).kafka_conductor()
        assert not BootStrategy(
            app, enable_kafka_consumer=False).kafka_consumer()
        assert not BootStrategy(
            app, enable_kafka_consumer=False).kafka_conductor()
        assert BootStrategy(
            app, enable_kafka=True).kafka_consumer()
        assert BootStrategy(
            app, enable_kafka=True).kafka_conductor()
        assert not BootStrategy(
            app, enable_kafka=False).kafka_consumer()
        assert not BootStrategy(
            app, enable_kafka=False).kafka_conductor()

    def test_kafka_producer(self, *, app):
        assert BootStrategy(
            app, enable_kafka_producer=True).kafka_producer()
        assert not BootStrategy(
            app, enable_kafka_producer=False).kafka_producer()
        assert BootStrategy(
            app, enable_kafka=True).kafka_producer()
        assert BootStrategy(
            app, enable_kafka_producer=None).kafka_producer()
        assert not BootStrategy(
            app, enable_kafka=False).kafka_producer()

    def test_web_server(self, *, app):
        assert BootStrategy(app, enable_web=True).web_server()
        assert not BootStrategy(app, enable_web=False).web_server()
        assert BootStrategy(app, enable_web=True).web_server()
        assert not BootStrategy(app, enable_web=False).web_server()
        assert BootStrategy(app, enable_web=None).web_server()

    def test_disable_kafka(self, *, app):
        class B(BootStrategy):
            enable_kafka = False

        b = B(app)
        assert not b.enable_kafka
        assert not b.kafka_conductor()
        assert not b.kafka_consumer()
        assert not b.kafka_producer()

    def test_disable_kafka_consumer(self, *, app):
        class B(BootStrategy):
            enable_kafka_consumer = False

        b = B(app)
        assert b.enable_kafka
        assert not b.kafka_conductor()
        assert not b.kafka_consumer()
        assert b.kafka_producer()
