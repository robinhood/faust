import socket
from pathlib import Path

import faust
import mode
import pytest
from aiohttp.client import ClientSession
from faust import App
from faust.assignor import LeaderAssignor, PartitionAssignor
from faust.exceptions import AlreadyConfiguredWarning, ImproperlyConfigured
from faust.app.router import Router
from faust.sensors import Monitor
from faust.serializers import Registry
from faust.tables import TableManager
from faust.types import settings
from yarl import URL


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
        assert conf.broker == URL(settings.BROKER_URL)
        assert conf.store == URL(settings.STORE_URL)
        assert conf.cache == URL(settings.CACHE_URL)
        assert conf.web == URL(settings.WEB_URL)
        assert conf.web_enabled
        assert conf.datadir == conf._prepare_datadir(settings.DATADIR)
        assert conf.tabledir == conf._prepare_tabledir(settings.TABLEDIR)
        assert conf.broker_client_id == settings.BROKER_CLIENT_ID
        assert conf.broker_session_timeout == settings.BROKER_SESSION_TIMEOUT
        assert (conf.broker_heartbeat_interval ==
                settings.BROKER_HEARTBEAT_INTERVAL)
        assert conf.broker_commit_interval == settings.BROKER_COMMIT_INTERVAL
        assert conf.broker_commit_every == settings.BROKER_COMMIT_EVERY
        assert (conf.broker_commit_livelock_soft_timeout ==
                settings.BROKER_LIVELOCK_SOFT)
        assert conf.broker_check_crcs
        assert conf.table_cleanup_interval == settings.TABLE_CLEANUP_INTERVAL
        assert conf.reply_to_prefix == settings.REPLY_TO_PREFIX
        assert conf.reply_expires == settings.REPLY_EXPIRES
        assert conf.stream_buffer_maxsize == settings.STREAM_BUFFER_MAXSIZE
        assert conf.stream_recovery_delay == settings.STREAM_RECOVERY_DELAY
        assert conf.producer_partitioner is None
        assert (conf.stream_publish_on_commit ==
                settings.STREAM_PUBLISH_ON_COMMIT)
        assert conf.stream_wait_empty
        assert not conf.stream_ack_cancelled_tasks
        assert conf.stream_ack_exceptions

        assert not conf.autodiscover
        assert conf.origin is None
        assert conf.key_serializer == 'raw'
        assert conf.value_serializer == 'json'
        assert conf.reply_to is not None
        assert not conf.reply_create_topic
        assert conf.table_standby_replicas == 1
        assert conf.topic_replication_factor == 1
        assert conf.topic_partitions == 8
        assert conf.loghandlers == []
        assert conf.version == 1
        assert conf.canonical_url == URL(f'http://{socket.gethostname()}:6066')
        assert conf.web_bind == '0.0.0.0'
        assert conf.web_port == 6066
        assert conf.web_transport == settings.WEB_TRANSPORT
        assert conf.worker_redirect_stdouts
        assert conf.worker_redirect_stdouts_level == 'WARN'

        assert conf.agent_supervisor is mode.OneForOneSupervisor

        assert conf.Agent is faust.Agent
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
        app = self.assert_config_equivalent(broker=URL('ckafka://'))
        assert isinstance(app.conf.broker, URL)

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
        app = self.assert_config_equivalent(datadir=Path('/etc/moo'))
        assert isinstance(app.conf.datadir, Path)

    def test_tabledir_is_relative_to_path(self):
        app = self.assert_config_equivalent(
            datadir='/etc/faust',
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
                                 datadir='/etc/faust/',
                                 tabledir='/var/faust/',
                                 broker_heartbeat_interval=101.13,
                                 broker_session_timeout=30303.30,
                                 broker_commit_every=202,
                                 broker_commit_interval=30.3,
                                 broker_commit_livelock_soft_timeout=60.6,
                                 broker_check_crcs=False,
                                 producer_partitioner=_dummy_partitioner,
                                 table_cleanup_interval=80.8,
                                 key_serializer='str',
                                 value_serializer='str',
                                 table_standby_replicas=48,
                                 topic_replication_factor=16,
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
                                 worker_redirect_stdouts=False,
                                 worker_redirect_stdouts_level='DEBUG',
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
            broker_session_timeout=broker_session_timeout,
            broker_heartbeat_interval=broker_heartbeat_interval,
            broker_commit_every=broker_commit_every,
            broker_commit_interval=broker_commit_interval,
            broker_commit_livelock_soft_timeout=livelock_soft_timeout,
            broker_check_crcs=broker_check_crcs,
            producer_partitioner=producer_partitioner,
            table_cleanup_interval=table_cleanup_interval,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            table_standby_replicas=table_standby_replicas,
            topic_replication_factor=topic_replication_factor,
            reply_to=reply_to,
            reply_create_topic=reply_create_topic,
            reply_expires=reply_expires,
            stream_buffer_maxsize=stream_buffer_maxsize,
            stream_wait_empty=stream_wait_empty,
            stream_ack_cancelled_tasks=stream_ack_cancelled_tasks,
            stream_ack_exceptions=stream_ack_exceptions,
            stream_publish_on_commit=stream_publish_on_commit,
            stream_recovery_delay=stream_recovery_delay,
            web_bind=web_bind,
            web_port=web_port,
            web_host=web_host,
            web_transport=web_transport,
            worker_redirect_stdouts=worker_redirect_stdouts,
            worker_redirect_stdouts_level=worker_redirect_stdouts_level,
        )
        conf = app.conf
        assert conf.id == app.conf._prepare_id(id)
        assert conf.broker == URL(str(broker))
        assert conf.store == URL(str(store))
        assert conf.cache == URL(str(cache))
        assert conf.web == URL(str(web))
        assert not conf.web_enabled
        assert conf.autodiscover == autodiscover
        assert conf.canonical_url == URL(str(canonical_url))
        assert conf.broker_client_id == broker_client_id
        assert conf.datadir == Path(str(datadir))
        if Path(tabledir).is_absolute():
            assert conf.tabledir == Path(str(tabledir))
        else:
            assert conf.tabledir.relative_to(conf.appdir) == Path(tabledir)
        assert conf.broker_heartbeat_interval == broker_heartbeat_interval
        assert conf.broker_session_timeout == broker_session_timeout
        assert conf.broker_commit_every == broker_commit_every
        assert conf.broker_commit_interval == broker_commit_interval
        assert (conf.broker_commit_livelock_soft_timeout ==
                broker_commit_livelock_soft_timeout)
        assert conf.broker_check_crcs == broker_check_crcs
        assert conf.producer_partitioner is producer_partitioner
        assert conf.table_cleanup_interval == table_cleanup_interval
        assert conf.key_serializer == key_serializer
        assert conf.value_serializer == value_serializer
        assert conf.table_standby_replicas == table_standby_replicas
        assert conf.topic_replication_factor == topic_replication_factor
        assert conf.reply_to == reply_to
        assert conf.reply_expires == reply_expires
        assert conf.stream_buffer_maxsize == stream_buffer_maxsize
        assert conf.stream_wait_empty == stream_wait_empty
        assert conf.stream_ack_cancelled_tasks == stream_ack_cancelled_tasks
        assert conf.stream_ack_exceptions == stream_ack_exceptions
        assert conf.stream_publish_on_commit == stream_publish_on_commit
        assert conf.stream_recovery_delay == stream_recovery_delay
        assert conf.web_bind == web_bind
        assert conf.web_port == web_port
        assert conf.web_host == web_host
        assert conf.web_transport == URL(web_transport)
        assert conf.worker_redirect_stdouts == worker_redirect_stdouts
        assert (conf.worker_redirect_stdouts_level ==
                worker_redirect_stdouts_level)
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
        assert self.App(url='foo').conf.broker == URL('kafka://foo')

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
        assert app.conf.broker.scheme == settings.DEFAULT_BROKER_SCHEME
        assert app.conf.broker.host == 'example.com'
        assert app.conf.broker.port == 3123
