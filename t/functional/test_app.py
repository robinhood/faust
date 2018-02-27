from pathlib import Path

import faust
import pytest
from faust import App
from faust.assignor.partition_assignor import PartitionAssignor
from faust.exceptions import ImproperlyConfigured
from faust.router import Router
from faust.serializers import Registry
from faust.tables import TableManager
from faust.types import settings
from yarl import URL


class test_settings:

    def test_defaults(self):
        app = App('myid')
        app.finalize()
        conf = app.conf
        assert conf.broker == URL(settings.BROKER_URL)
        assert conf.store == URL(settings.STORE_URL)
        assert conf.datadir == conf.prepare_datadir(settings.DATADIR)
        assert conf.tabledir == conf.prepare_tabledir(settings.TABLEDIR)
        assert conf.broker_client_id == settings.BROKER_CLIENT_ID
        assert conf.broker_commit_interval == settings.BROKER_COMMIT_INTERVAL
        assert conf.table_cleanup_interval == settings.TABLE_CLEANUP_INTERVAL
        assert conf.reply_to_prefix == settings.REPLY_TO_PREFIX
        assert conf.reply_expires == settings.REPLY_EXPIRES
        assert conf.stream_buffer_maxsize == settings.STREAM_BUFFER_MAXSIZE

        assert not conf.autodiscover
        assert conf.origin is None
        assert conf.key_serializer == 'json'
        assert conf.value_serializer == 'json'
        assert conf.reply_to is not None
        assert not conf.reply_create_topic
        assert conf.table_standby_replicas == 1
        assert conf.topic_replication_factor == 1
        assert conf.topic_partitions == 8
        assert conf.loghandlers is None
        assert conf.version == 1
        assert conf.canonical_url is None

        assert conf.Stream is faust.Stream
        assert conf.Table is faust.Table
        assert conf.TableManager is TableManager
        assert conf.Set is faust.Set
        assert conf.Serializers is Registry
        assert conf.Worker is faust.Worker
        assert conf.PartitionAssignor is PartitionAssignor
        assert conf.Router is Router

    def test_reply_prefix_unique(self):
        app1 = App('myid1')
        app1.finalize()
        app2 = App('myid1')
        app2.finalize()
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
        assert app.conf.tabledir == app.conf.datadir / Path('moo')

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
                                 broker_commit_interval=30.3,
                                 table_cleanup_interval=80.8,
                                 key_serializer='str',
                                 value_serializer='str',
                                 table_standby_replicas=48,
                                 topic_replication_factor=16,
                                 reply_to='reply_to',
                                 reply_create_topic=True,
                                 reply_expires=90.9,
                                 stream_buffer_maxsize=101,
                                 **kwargs) -> App:
        app = App(
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
            broker_commit_interval=broker_commit_interval,
            table_cleanup_interval=table_cleanup_interval,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            table_standby_replicas=table_standby_replicas,
            topic_replication_factor=topic_replication_factor,
            reply_to=reply_to,
            reply_create_topic=reply_create_topic,
            reply_expires=reply_expires,
            stream_buffer_maxsize=stream_buffer_maxsize,
        )
        app.finalize()
        assert app.conf.id == app.conf.prepare_id(id)
        assert app.conf.broker == URL(str(broker))
        assert app.conf.store == URL(str(store))
        assert app.conf.autodiscover == autodiscover
        assert app.conf.canonical_url == URL(str(canonical_url))
        assert app.conf.broker_client_id == broker_client_id
        assert app.conf.datadir == Path(str(datadir))
        if Path(tabledir).is_absolute():
            assert app.conf.tabledir == Path(str(tabledir))
        else:
            assert app.conf.tabledir.relative_to(
                app.conf.datadir) == Path(tabledir)
        assert app.conf.broker_commit_interval == broker_commit_interval
        assert app.conf.table_cleanup_interval == table_cleanup_interval
        assert app.conf.key_serializer == key_serializer
        assert app.conf.value_serializer == value_serializer
        assert app.conf.table_standby_replicas == table_standby_replicas
        assert app.conf.topic_replication_factor == topic_replication_factor
        assert app.conf.reply_to == reply_to
        assert app.conf.reply_expires == reply_expires
        assert app.conf.stream_buffer_maxsize == stream_buffer_maxsize
        return app

    def test_id_no_version(self):
        app = App('id', version=1)
        app.finalize()
        assert app.conf.id == 'id'

    def test_version_cannot_be_zero(self):
        app = App('id', version=0)
        with pytest.raises(ImproperlyConfigured):
            app.finalize()
