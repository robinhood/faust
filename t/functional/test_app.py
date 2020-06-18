import socket
import sys
from typing import Any, Mapping, NamedTuple
from pathlib import Path

import faust
import mode
import pytest
import pytz

from mode.supervisors import OneForAllSupervisor
from mode.utils.mocks import patch
from yarl import URL

from faust import App
from faust.app import BootStrategy
from faust.assignor import LeaderAssignor, PartitionAssignor
from faust.exceptions import AlreadyConfiguredWarning, ImproperlyConfigured
from faust.app.router import Router
from faust.sensors import Monitor
from faust.serializers import Registry
from faust.tables import TableManager
from faust.transport.utils import DefaultSchedulingStrategy
from faust.types.enums import ProcessingGuarantee
from faust.types.settings import Settings
from faust.types.settings.params import Param
from faust.types.web import ResourceOptions

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


class EnvCase(NamedTuple):
    env: Mapping[str, str]
    setting: Param
    expected_value: Any


class test_settings:

    def App(self, id='myid', **kwargs):
        app = App(id, **kwargs)
        app.finalize()
        return app

    def test_env_with_prefix(self):
        env = {
            'FOO_BROKER_URL': 'foobar://',
        }
        app = self.App(env=env, env_prefix='FOO_', broker='xaz://')
        assert app.conf.broker == [URL('foobar://')]

    @pytest.mark.parametrize('env,setting,expected_value', [
        EnvCase(
            env={'APP_DATADIR': '/foo/bar/baz'},
            setting=Settings.datadir,
            expected_value=Path('/foo/bar/baz'),
        ),
        EnvCase(
            env={'APP_TABLEDIR': '/foo/bar/bax'},
            setting=Settings.tabledir,
            expected_value=Path('/foo/bar/bax'),
        ),
        EnvCase(
            env={'APP_DEBUG': 'yes'},
            setting=Settings.debug,
            expected_value=True,
        ),
        EnvCase(
            env={'APP_DEBUG': 'no'},
            setting=Settings.debug,
            expected_value=False,
        ),
        EnvCase(
            env={'APP_DEBUG': '0'},
            setting=Settings.debug,
            expected_value=False,
        ),
        EnvCase(
            env={'APP_DEBUG': ''},
            setting=Settings.debug,
            expected_value=False,
        ),
        EnvCase(
            env={'TIMEZONE': 'Europe/Berlin'},
            setting=Settings.timezone,
            expected_value=pytz.timezone('Europe/Berlin'),
        ),
        EnvCase(
            env={'APP_VERSION': '3'},
            setting=Settings.version,
            expected_value=3,
        ),
        EnvCase(
            env={'AGENT_SUPERVISOR': 'mode.supervisors.OneForAllSupervisor'},
            setting=Settings.agent_supervisor,
            expected_value=OneForAllSupervisor,
        ),
        EnvCase(
            env={'BLOCKING_TIMEOUT': '0.0'},
            setting=Settings.blocking_timeout,
            expected_value=0.0,
        ),
        EnvCase(
            env={'BLOCKING_TIMEOUT': '3.03'},
            setting=Settings.blocking_timeout,
            expected_value=3.03,
        ),
        EnvCase(
            env={'BROKER_URL': 'foo://'},
            setting=Settings.broker,
            expected_value=[URL('foo://')],
        ),
        EnvCase(
            env={'BROKER_URL': 'foo://a;foo://b;foo://c'},
            setting=Settings.broker,
            expected_value=[URL('foo://a'), URL('foo://b'), URL('foo://c')],
        ),
        EnvCase(
            env={'BROKER_URL': 'foo://a;foo://b;foo://c'},
            setting=Settings.broker_consumer,
            expected_value=[URL('foo://a'), URL('foo://b'), URL('foo://c')],
        ),
        EnvCase(
            env={'BROKER_URL': 'foo://a;foo://b;foo://c'},
            setting=Settings.broker_producer,
            expected_value=[URL('foo://a'), URL('foo://b'), URL('foo://c')],
        ),
        EnvCase(
            env={'BROKER_CONSUMER_URL': 'foo://a;foo://b;foo://c'},
            setting=Settings.broker_consumer,
            expected_value=[URL('foo://a'), URL('foo://b'), URL('foo://c')],
        ),
        EnvCase(
            env={'BROKER_PRODUCER_URL': 'foo://a;foo://b;foo://c'},
            setting=Settings.broker_producer,
            expected_value=[URL('foo://a'), URL('foo://b'), URL('foo://c')],
        ),
        EnvCase(
            env={'BROKER_API_VERSION': '1.12'},
            setting=Settings.broker_api_version,
            expected_value='1.12',
        ),
        EnvCase(
            env={'BROKER_API_VERSION': '1.12'},
            setting=Settings.consumer_api_version,
            expected_value='1.12',
        ),
        EnvCase(
            env={'CONSUMER_API_VERSION': '1.13'},
            setting=Settings.consumer_api_version,
            expected_value='1.13',
        ),
        EnvCase(
            env={'BROKER_API_VERSION': '1.12'},
            setting=Settings.producer_api_version,
            expected_value='1.12',
        ),
        EnvCase(
            env={'PRODUCER_API_VERSION': '1.14'},
            setting=Settings.producer_api_version,
            expected_value='1.14',
        ),
        EnvCase(
            env={'BROKER_CHECK_CRCS': 'no'},
            setting=Settings.broker_check_crcs,
            expected_value=False,
        ),
        EnvCase(
            env={'BROKER_CLIENT_ID': 'x-y-z'},
            setting=Settings.broker_client_id,
            expected_value='x-y-z',
        ),
        EnvCase(
            env={'BROKER_COMMIT_EVERY': '10'},
            setting=Settings.broker_commit_every,
            expected_value=10,
        ),
        EnvCase(
            env={'BROKER_COMMIT_INTERVAL': '10'},
            setting=Settings.broker_commit_interval,
            expected_value=10.0,
        ),
        EnvCase(
            env={'BROKER_COMMIT_INTERVAL': '10.1234'},
            setting=Settings.broker_commit_interval,
            expected_value=10.1234,
        ),
        EnvCase(
            env={'BROKER_COMMIT_LIVELOCK_SOFT_TIMEOUT': '10.1234'},
            setting=Settings.broker_commit_livelock_soft_timeout,
            expected_value=10.1234,
        ),
        EnvCase(
            env={'BROKER_HEARTBEAT_INTERVAL': '10.1234'},
            setting=Settings.broker_heartbeat_interval,
            expected_value=10.1234,
        ),
        EnvCase(
            env={'BROKER_MAX_POLL_INTERVAL': '10.1234'},
            setting=Settings.broker_max_poll_interval,
            expected_value=10.1234,
        ),
        EnvCase(
            env={'BROKER_MAX_POLL_RECORDS': '30'},
            setting=Settings.broker_max_poll_records,
            expected_value=30,
        ),
        EnvCase(
            env={'BROKER_REBALANCE_TIMEOUT': '10.1234'},
            setting=Settings.broker_rebalance_timeout,
            expected_value=10.1234,
        ),
        EnvCase(
            env={'BROKER_REQUEST_TIMEOUT': '10.1234'},
            setting=Settings.broker_request_timeout,
            expected_value=10.1234,
        ),
        EnvCase(
            env={'BROKER_SESSION_TIMEOUT': '10.1234'},
            setting=Settings.broker_session_timeout,
            expected_value=10.1234,
        ),
        EnvCase(
            env={'CONSUMER_MAX_FETCH_SIZE': '123942012'},
            setting=Settings.consumer_max_fetch_size,
            expected_value=123942012,
        ),
        EnvCase(
            env={'CONSUMER_AUTO_OFFSET_RESET': 'latest'},
            setting=Settings.consumer_auto_offset_reset,
            expected_value='latest',
        ),
        EnvCase(
            env={'APP_KEY_SERIALIZER': 'yaml'},
            setting=Settings.key_serializer,
            expected_value='yaml',
        ),
        EnvCase(
            env={'APP_VALUE_SERIALIZER': 'yaml'},
            setting=Settings.value_serializer,
            expected_value='yaml',
        ),
        EnvCase(
            env={'PRODUCER_ACKS': '0'},
            setting=Settings.producer_acks,
            expected_value=0,
        ),
        EnvCase(
            env={'PRODUCER_ACKS': '1'},
            setting=Settings.producer_acks,
            expected_value=1,
        ),
        EnvCase(
            env={'PRODUCER_ACKS': '-1'},
            setting=Settings.producer_acks,
            expected_value=-1,
        ),
        EnvCase(
            env={'PRODUCER_COMPRESSION_TYPE': 'snappy'},
            setting=Settings.producer_compression_type,
            expected_value='snappy',
        ),
        EnvCase(
            env={'PRODUCER_LINGER_MS': '120392'},
            setting=Settings.producer_linger,
            expected_value=120.392,
        ),
        EnvCase(
            env={'PRODUCER_LINGER': '12.345'},
            setting=Settings.producer_linger,
            expected_value=12.345,
        ),
        EnvCase(
            env={'PRODUCER_MAX_BATCH_SIZE': '120392'},
            setting=Settings.producer_max_batch_size,
            expected_value=120392,
        ),
        EnvCase(
            env={'PRODUCER_MAX_REQUEST_SIZE': '120392'},
            setting=Settings.producer_max_request_size,
            expected_value=120392,
        ),
        EnvCase(
            env={'PRODUCER_REQUEST_TIMEOUT': '120.392'},
            setting=Settings.producer_request_timeout,
            expected_value=120.392,
        ),
        EnvCase(
            env={'APP_REPLY_CREATE_TOPIC': '1'},
            setting=Settings.reply_create_topic,
            expected_value=True,
        ),
        EnvCase(
            env={'APP_REPLY_EXPIRES': '13.321'},
            setting=Settings.reply_expires,
            expected_value=13.321,
        ),
        EnvCase(
            env={'APP_REPLY_TO_PREFIX': 'foo-bar-baz'},
            setting=Settings.reply_to_prefix,
            expected_value='foo-bar-baz',
        ),
        EnvCase(
            env={'PROCESSING_GUARANTEE': 'exactly_once'},
            setting=Settings.processing_guarantee,
            expected_value=ProcessingGuarantee.EXACTLY_ONCE,
        ),
        EnvCase(
            env={'PROCESSING_GUARANTEE': 'at_least_once'},
            setting=Settings.processing_guarantee,
            expected_value=ProcessingGuarantee.AT_LEAST_ONCE,
        ),
        EnvCase(
            env={'STREAM_BUFFER_MAXSIZE': '16384'},
            setting=Settings.stream_buffer_maxsize,
            expected_value=16384,
        ),
        EnvCase(
            env={'STREAM_PROCESSING_TIMEOUT': '12.312'},
            setting=Settings.stream_processing_timeout,
            expected_value=12.312,
        ),
        EnvCase(
            env={'STREAM_RECOVERY_DELAY': '12.312'},
            setting=Settings.stream_recovery_delay,
            expected_value=12.312,
        ),
        EnvCase(
            env={'STREAM_WAIT_EMPTY': 'no'},
            setting=Settings.stream_wait_empty,
            expected_value=False,
        ),
        EnvCase(
            env={'STREAM_WAIT_EMPTY': 'yes'},
            setting=Settings.stream_wait_empty,
            expected_value=True,
        ),
        EnvCase(
            env={'APP_STORE': 'rocksdb://'},
            setting=Settings.store,
            expected_value=URL('rocksdb://'),
        ),
        EnvCase(
            env={'TABLE_CLEANUP_INTERVAL': '60.03'},
            setting=Settings.table_cleanup_interval,
            expected_value=60.03,
        ),
        EnvCase(
            env={'TABLE_KEY_INDEX_SIZE': '32030'},
            setting=Settings.table_key_index_size,
            expected_value=32030,
        ),
        EnvCase(
            env={'TABLE_STANDBY_REPLICAS': '10'},
            setting=Settings.table_standby_replicas,
            expected_value=10,
        ),
        EnvCase(
            env={'TOPIC_ALLOW_DECLARE': '0'},
            setting=Settings.topic_allow_declare,
            expected_value=False,
        ),
        EnvCase(
            env={'TOPIC_ALLOW_DECLARE': '1'},
            setting=Settings.topic_allow_declare,
            expected_value=True,
        ),
        EnvCase(
            env={'TOPIC_DISABLE_LEADER': '0'},
            setting=Settings.topic_disable_leader,
            expected_value=False,
        ),
        EnvCase(
            env={'TOPIC_DISABLE_LEADER': '1'},
            setting=Settings.topic_disable_leader,
            expected_value=True,
        ),
        EnvCase(
            env={'TOPIC_PARTITIONS': '100'},
            setting=Settings.topic_partitions,
            expected_value=100,
        ),
        EnvCase(
            env={'TOPIC_REPLICATION_FACTOR': '100'},
            setting=Settings.topic_replication_factor,
            expected_value=100,
        ),
        EnvCase(
            env={'CACHE_URL': 'redis://'},
            setting=Settings.cache,
            expected_value=URL('redis://'),
        ),
        EnvCase(
            env={'WEB_BIND': '0.0.0.0'},
            setting=Settings.web_bind,
            expected_value='0.0.0.0',
        ),
        EnvCase(
            env={'APP_WEB_ENABLED': 'no'},
            setting=Settings.web_enabled,
            expected_value=False,
        ),
        EnvCase(
            env={'WEB_HOST': 'foo.bar.com'},
            setting=Settings.web_host,
            expected_value='foo.bar.com',
        ),
        EnvCase(
            env={'WEB_HOST': 'foo.bar.com'},
            setting=Settings.canonical_url,
            expected_value=URL('http://foo.bar.com:6066'),
        ),
        EnvCase(
            env={'WEB_PORT': '303'},
            setting=Settings.web_port,
            expected_value=303,
        ),
        EnvCase(
            env={'WEB_PORT': '303', 'WEB_HOST': 'foo.bar.com'},
            setting=Settings.canonical_url,
            expected_value=URL('http://foo.bar.com:303'),
        ),
        EnvCase(
            env={'WORKER_REDIRECT_STDOUTS': 'no'},
            setting=Settings.worker_redirect_stdouts,
            expected_value=False,
        ),
        EnvCase(
            env={'WORKER_REDIRECT_STDOUTS_LEVEL': 'error'},
            setting=Settings.worker_redirect_stdouts_level,
            expected_value='error',
        ),
    ])
    def test_env(self, env, setting, expected_value):
        app = self.App(env=env)
        self.assert_expected(setting.__get__(app.conf), expected_value)

        # env prefix passed as argument
        prefixed_env = {'FOO_' + k: v for k, v in env.items()}
        app2 = self.App(env=prefixed_env, env_prefix='FOO_')
        self.assert_expected(setting.__get__(app2.conf), expected_value)

        # env prefix set in ENV
        prefixed_env2 = {'BAR_' + k: v for k, v in env.items()}
        prefixed_env2['APP_ENV_PREFIX'] = 'BAR_'
        app3 = self.App(env=prefixed_env2)
        assert app3.conf.env_prefix == 'BAR_'
        self.assert_expected(setting.__get__(app3.conf), expected_value)

    def assert_expected(self, value, expected_value):
        if expected_value is None:
            assert value is None
        elif expected_value is True:
            assert value is True
        elif expected_value is False:
            assert value is False
        else:
            assert value == expected_value

    def test_defaults(self):
        app = self.App()
        conf = app.conf
        assert not conf.debug
        assert conf.broker == [URL(Settings.DEFAULT_BROKER_URL)]
        assert conf.broker_consumer == [URL(Settings.DEFAULT_BROKER_URL)]
        assert conf.broker_producer == [URL(Settings.DEFAULT_BROKER_URL)]
        assert conf.store == URL(Settings.store.default)
        assert conf.cache == URL(Settings.cache.default)
        assert conf.web == URL(Settings.web.default)
        assert conf.web_enabled
        assert not conf.web_in_thread
        assert conf.datadir == Path('myid-data')
        assert conf.tabledir == Path('myid-data/v1/tables')
        assert conf.blocking_timeout is None
        assert conf.processing_guarantee == ProcessingGuarantee.AT_LEAST_ONCE
        assert conf.broker_api_version == Settings.broker_api_version.default
        assert conf.broker_client_id == Settings.broker_client_id.default
        assert (conf.broker_request_timeout ==
                Settings.broker_request_timeout.default)
        assert (conf.broker_session_timeout ==
                Settings.broker_session_timeout.default)
        assert (conf.broker_rebalance_timeout ==
                Settings.broker_rebalance_timeout.default)
        assert (conf.broker_heartbeat_interval ==
                Settings.broker_heartbeat_interval.default)
        assert (conf.broker_commit_interval ==
                Settings.broker_commit_interval.default)
        assert (conf.broker_commit_every ==
                Settings.broker_commit_every.default)
        assert (conf.broker_commit_livelock_soft_timeout ==
                Settings.broker_commit_livelock_soft_timeout.default)
        assert conf.broker_check_crcs
        assert (conf.consumer_api_version ==
                Settings.broker_api_version.default)
        assert conf.timezone is Settings.timezone.default
        assert (conf.table_cleanup_interval ==
                Settings.table_cleanup_interval.default)
        assert (conf.table_key_index_size ==
                Settings.table_key_index_size.default)
        assert conf.reply_to_prefix == Settings.reply_to_prefix.default
        assert conf.reply_expires == Settings.reply_expires.default
        assert (conf.stream_buffer_maxsize ==
                Settings.stream_buffer_maxsize.default)
        assert (conf.stream_recovery_delay ==
                Settings.stream_recovery_delay.default)
        assert (conf.stream_processing_timeout ==
                Settings.stream_processing_timeout.default)
        assert (conf.producer_partitioner ==
                Settings.producer_partitioner.default)
        assert (conf.producer_request_timeout ==
                Settings.producer_request_timeout.default)
        assert (conf.producer_api_version ==
                Settings.broker_api_version.default)
        assert conf.producer_linger == 0.0
        assert (conf.stream_publish_on_commit ==
                Settings.stream_publish_on_commit.default)
        assert conf.stream_wait_empty
        assert (conf.broker_max_poll_records ==
                Settings.broker_max_poll_records.default)
        assert (conf.broker_max_poll_interval ==
                Settings.broker_max_poll_interval.default)
        assert (conf.consumer_auto_offset_reset ==
                Settings.consumer_auto_offset_reset.default)
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
        assert not conf.topic_disable_leader
        assert conf.logging_config is None
        assert conf.loghandlers == []
        assert conf.version == 1
        assert conf.canonical_url == URL(f'http://{socket.gethostname()}:6066')
        assert conf.web_bind == '0.0.0.0'
        assert conf.web_port == 6066
        assert conf.web_transport == Settings.web_transport.default
        assert conf.web_cors_options is None
        assert conf.worker_redirect_stdouts
        assert conf.worker_redirect_stdouts_level == 'WARN'

        assert conf.agent_supervisor is mode.OneForOneSupervisor

        assert conf.Agent is faust.Agent
        assert conf.ConsumerScheduler is DefaultSchedulingStrategy
        assert conf.Event is faust.Event
        assert conf.Schema is faust.Schema
        assert conf.Stream is faust.Stream
        assert conf.Table is faust.Table
        assert conf.TableManager is TableManager
        assert conf.Serializers is Registry
        assert conf.Worker is faust.Worker
        assert conf.PartitionAssignor is PartitionAssignor
        assert conf.LeaderAssignor is LeaderAssignor
        assert conf.Router is Router
        assert conf.Topic is faust.Topic
        from aiohttp.client import ClientSession
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
                                 blocking_timeout=3.03,
                                 broker_api_version='0.1',
                                 broker_request_timeout=10000.05,
                                 broker_heartbeat_interval=101.13,
                                 broker_session_timeout=30303.30,
                                 broker_rebalance_timeout=606606.60,
                                 broker_commit_every=202,
                                 broker_commit_interval=30.3,
                                 broker_commit_livelock_soft_timeout=60.6,
                                 broker_check_crcs=False,
                                 broker_producer='moo://',
                                 broker_consumer='zoo://',
                                 consumer_api_version='0.4',
                                 producer_partitioner=_dummy_partitioner,
                                 producer_request_timeout=2.66,
                                 producer_api_version='0.10',
                                 producer_linger=3.0303,
                                 table_cleanup_interval=80.8,
                                 table_key_index_size=1999,
                                 key_serializer='str',
                                 value_serializer='str',
                                 table_standby_replicas=48,
                                 topic_replication_factor=16,
                                 topic_allow_declare=False,
                                 topic_disable_leader=True,
                                 reply_to='reply_to',
                                 reply_create_topic=True,
                                 reply_expires=90.9,
                                 stream_buffer_maxsize=101,
                                 stream_wait_empty=True,
                                 stream_publish_on_commit=False,
                                 stream_recovery_delay=69.3,
                                 stream_processing_timeout=73.03,
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
                                 broker_max_poll_interval=10000,
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
            broker_consumer=broker_consumer,
            broker_producer=broker_producer,
            store=store,
            cache=cache,
            web=web,
            web_enabled=web_enabled,
            autodiscover=autodiscover,
            origin=origin,
            canonical_url=canonical_url,
            blocking_timeout=blocking_timeout,
            broker_client_id=broker_client_id,
            datadir=datadir,
            tabledir=tabledir,
            processing_guarantee=processing_guarantee,
            broker_api_version=broker_api_version,
            broker_request_timeout=broker_request_timeout,
            broker_session_timeout=broker_session_timeout,
            broker_rebalance_timeout=broker_rebalance_timeout,
            broker_heartbeat_interval=broker_heartbeat_interval,
            broker_commit_every=broker_commit_every,
            broker_commit_interval=broker_commit_interval,
            broker_commit_livelock_soft_timeout=livelock_soft_timeout,
            broker_check_crcs=broker_check_crcs,
            broker_max_poll_records=broker_max_poll_records,
            broker_max_poll_interval=broker_max_poll_interval,
            consumer_api_version=consumer_api_version,
            producer_partitioner=producer_partitioner,
            producer_request_timeout=producer_request_timeout,
            producer_api_version=producer_api_version,
            producer_linger=producer_linger,
            table_cleanup_interval=table_cleanup_interval,
            table_key_index_size=table_key_index_size,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            table_standby_replicas=table_standby_replicas,
            topic_replication_factor=topic_replication_factor,
            topic_allow_declare=topic_allow_declare,
            topic_disable_leader=topic_disable_leader,
            reply_to=reply_to,
            reply_create_topic=reply_create_topic,
            reply_expires=reply_expires,
            stream_buffer_maxsize=stream_buffer_maxsize,
            stream_wait_empty=stream_wait_empty,
            stream_publish_on_commit=stream_publish_on_commit,
            stream_recovery_delay=stream_recovery_delay,
            stream_processing_timeout=stream_processing_timeout,
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
        assert conf.broker_consumer == [URL(broker_consumer)]
        assert conf.broker_producer == [URL(broker_producer)]
        assert conf.blocking_timeout == blocking_timeout
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
        assert conf.broker_api_version == broker_api_version
        assert conf.broker_request_timeout == broker_request_timeout
        assert conf.broker_heartbeat_interval == broker_heartbeat_interval
        assert conf.broker_session_timeout == broker_session_timeout
        assert conf.broker_rebalance_timeout == broker_rebalance_timeout
        assert conf.broker_commit_every == broker_commit_every
        assert conf.broker_commit_interval == broker_commit_interval
        assert (conf.broker_commit_livelock_soft_timeout ==
                broker_commit_livelock_soft_timeout)
        assert conf.broker_check_crcs == broker_check_crcs
        assert conf.consumer_api_version == consumer_api_version
        assert conf.producer_partitioner is producer_partitioner
        assert conf.producer_request_timeout == producer_request_timeout
        assert conf.producer_api_version == producer_api_version
        assert conf.producer_linger == producer_linger
        assert conf.table_cleanup_interval == table_cleanup_interval
        assert conf.table_key_index_size == table_key_index_size
        assert conf.key_serializer == key_serializer
        assert conf.value_serializer == value_serializer
        assert conf.table_standby_replicas == table_standby_replicas
        assert conf.topic_replication_factor == topic_replication_factor
        assert conf.topic_allow_declare == topic_allow_declare
        assert conf.topic_disable_leader == topic_disable_leader
        assert conf.reply_to == reply_to
        assert conf.reply_expires == reply_expires
        assert conf.stream_buffer_maxsize == stream_buffer_maxsize
        assert conf.stream_wait_empty == stream_wait_empty
        assert conf.stream_publish_on_commit == stream_publish_on_commit
        assert conf.stream_recovery_delay == stream_recovery_delay
        assert conf.stream_processing_timeout == stream_processing_timeout
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
        assert conf.broker_max_poll_interval == broker_max_poll_interval
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

    def test_producer_linger_ms__compat(self):
        app = self.App(producer_linger_ms=30303)
        assert app.conf.producer_linger == 30.303

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

    def test_compat_stream_ack_cancelled_tasks(self):
        with pytest.warns(UserWarning):
            assert not self.App(
                stream_ack_cancelled_tasks=False,
            ).conf.stream_ack_cancelled_tasks

    def test_compat_stream_ack_exceptions(self):
        with pytest.warns(UserWarning):
            assert self.App(
                stream_ack_exceptions=True).conf.stream_ack_exceptions

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
        assert url.scheme == Settings.broker.default_scheme
        assert url.host == 'example.com'
        assert url.port == 3123

    def test_consumer_api_version__defaults_to_broker(self):
        expected_broker_version = '0.3333'
        app = self.App(
            broker_api_version=expected_broker_version,
            consumer_api_version=None,
        )
        assert app.conf.consumer_api_version == expected_broker_version

    def test_producer_api_version__defaults_to_broker(self):
        expected_broker_version = '0.3333'
        app = self.App(
            broker_api_version=expected_broker_version,
            producer_api_version=None,
        )
        assert app.conf.producer_api_version == expected_broker_version


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

    @pytest.mark.app(debug=True)
    @pytest.mark.asyncio
    async def test_debug_enabled_warns_on_start(self, *, app):
        with patch('faust.app.base.logger') as logger:
            await app.on_start()
            logger.warning.assert_called_once()
