from pathlib import Path

import faust
import mode
import pytest
from faust import App
from faust.assignor import LeaderAssignor, PartitionAssignor
from faust.exceptions import ImproperlyConfigured
from faust.app.router import Router
from faust.sensors import Monitor
from faust.serializers import Registry
from faust.tables import TableManager
from faust.types import settings
from faust.types.app import HttpClientT
from yarl import URL


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
        assert conf.datadir == conf.prepare_datadir(settings.DATADIR)
        assert conf.tabledir == conf.prepare_tabledir(settings.TABLEDIR)
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
        assert (conf.stream_publish_on_commit ==
                settings.STREAM_PUBLISH_ON_COMMIT)
        assert not conf.stream_wait_empty
        assert not conf.stream_ack_cancelled_tasks
        assert conf.stream_ack_exceptions

        assert not conf.autodiscover
        assert conf.origin is None
        assert conf.key_serializer == 'json'
        assert conf.value_serializer == 'json'
        assert conf.reply_to is not None
        assert not conf.reply_create_topic
        assert conf.table_standby_replicas == 1
        assert conf.topic_replication_factor == 1
        assert conf.topic_partitions == 8
        assert conf.loghandlers == []
        assert conf.version == 1
        assert conf.canonical_url == URL('')
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
        assert conf.HttpClient is HttpClientT
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
                                 worker_redirect_stdouts=False,
                                 worker_redirect_stdouts_level='DEBUG',
                                 **kwargs) -> App:
        livelock_soft_timeout = broker_commit_livelock_soft_timeout
        app = self.App(
            id,
            version=version,
            broker=broker,
            store=store,
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
            worker_redirect_stdouts=worker_redirect_stdouts,
            worker_redirect_stdouts_level=worker_redirect_stdouts_level,
        )
        conf = app.conf
        assert conf.id == app.conf.prepare_id(id)
        assert conf.broker == URL(str(broker))
        assert conf.store == URL(str(store))
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
        assert conf.worker_redirect_stdouts == worker_redirect_stdouts
        assert (conf.worker_redirect_stdouts_level ==
                worker_redirect_stdouts_level)
        return app

    def test_id_no_version(self):
        assert self.App('id', version=1).conf.id == 'id'

    def test_version_cannot_be_zero(self):
        app = App('id', version=0)
        with pytest.raises(ImproperlyConfigured):
            app.finalize()

    def test_compat_url(self):
        assert self.App(url='foo').conf.broker == URL('foo')

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
