import abc
import asyncio
import logging
import typing
from datetime import timedelta
from pathlib import Path
from typing import Callable, Iterable, List, Type, Union
from uuid import uuid4
from mode import Seconds, want_seconds
from mode.utils.imports import SymbolArg, symbol_by_name
from yarl import URL
from .. import __version__ as faust_version
from ..cli._env import DATADIR
from ..exceptions import ImproperlyConfigured
from ..types import CodecArg
from ..types.assignor import PartitionAssignorT
from ..types.router import RouterT
from ..types.serializers import RegistryT
from ..types.streams import StreamT
from ..types.tables import SetT, TableManagerT, TableT

if typing.TYPE_CHECKING:
    from .worker import Worker as WorkerT
else:
    class WorkerT: ...     # noqa

__all__ = ['AutodiscoverArg', 'Settings']

#: Broker URL, used as default for ``app.conf.broker``.
BROKER_URL = 'kafka://localhost:9092'

#: Table storage URL, used as default for ``app.conf.store``.
STORE_URL = 'memory://'

#: Table state directory path used as default for ``app.conf.tabledir``.
#: This path will be treated as relative to datadir, unless the provided
#: poth is absolute.
TABLEDIR = 'tables'

#: Path to stream class, used as default for ``app.conf.Stream``.
STREAM_TYPE = 'faust.Stream'

#: Path to table manager class, used as default for ``app.conf.TableManager``.
TABLE_MANAGER_TYPE = 'faust.tables.TableManager'

#: Path to table class, used as default for ``app.conf.Table``.
TABLE_TYPE = 'faust.Table'

#: Path to set class, used as default for ``app.conf.Set``.
SET_TYPE = 'faust.Set'

#: Path to serializer registry class, used as the default for
#: ``app.conf.Serializers``.
REGISTRY_TYPE = 'faust.serializers.Registry'

#: Path to worker class, providing the default for ``app.conf.Worker``.
WORKER_TYPE = 'faust.worker.Worker'

#: Path to partition assignor class, providing the default for
#: ``app.conf.PartitionAssignor``.
PARTITION_ASSIGNOR_TYPE = 'faust.assignor:PartitionAssignor'

#: Path to router class, providing the default for ``app.conf.Router``.
ROUTER_TYPE = 'faust.router:Router'

#: Default Kafka Client ID.
CLIENT_ID = f'faust-{faust_version}'

#: How often we commit acknowledged messages.
#: Used as the default value for the :attr:`App.conf.commit_interval` argument.
COMMIT_INTERVAL = 3.0

#: How often we clean up expired items in windowed tables.
#: Used as the default value for the :attr:`App.conf.table_cleanup_interval`
#: argument.
TABLE_CLEANUP_INTERVAL = 30.0

#: Prefix used for reply topics.
REPLY_TOPIC_PREFIX = 'f-reply-'

#: Default expiry time for replies, in seconds (float).
REPLY_EXPIRES = want_seconds(timedelta(days=1))

#: Max number of messages channels/streams/topics can "prefetch".
STREAM_BUFFER_MAXSIZE = 1000

AutodiscoverArg = Union[
    bool,
    Iterable[str],
    Callable[[], Iterable[str]],
]


class Settings(abc.ABC):
    autodiscover: AutodiscoverArg = False
    client_id: str = CLIENT_ID
    origin: str = None
    key_serializer: CodecArg = 'json'
    value_serializer: CodecArg = 'json'
    num_standby_replicas: int = 1
    replication_factor: int = 1
    default_partitions: int = 8  # noqa: E704
    id_format: str = '{id}-v{self.version}'
    reply_to: str = None
    reply_to_prefix: str = REPLY_TOPIC_PREFIX
    create_reply_topic: bool = False
    stream_buffer_maxsize: int = STREAM_BUFFER_MAXSIZE
    loghandlers: List[logging.StreamHandler] = None
    loop: asyncio.AbstractEventLoop = None

    _id: str = None
    _version: int = 1
    _broker: URL = None
    _store: URL = None
    _canonical_url: URL = None
    _datadir: Path = None
    _tabledir: Path = None
    _commit_interval: float = None
    _table_cleanup_interval: float = None
    _reply_expires: float = None
    _Stream: Type[StreamT] = None
    _Table: Type[TableT] = None
    _TableManager: Type[TableManagerT] = None
    _Set: Type[SetT] = None
    _Serializers: Type[RegistryT] = None
    _Worker: Type[WorkerT] = None
    _PartitionAssignor: Type[PartitionAssignorT] = None
    _Router: Type[RouterT] = None

    def __init__(
            self, id: str,
            *,
            version: int = None,
            broker: Union[str, URL] = None,
            store: Union[str, URL] = None,
            autodiscover: AutodiscoverArg = None,
            origin: str = None,
            canonical_url: Union[str, URL] = None,
            client_id: str = None,
            datadir: Union[Path, str] = None,
            tabledir: Union[Path, str] = None,
            commit_interval: Seconds = None,
            table_cleanup_interval: Seconds = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            num_standby_replicas: int = None,
            replication_factor: int = None,
            default_partitions: int = None,
            id_format: str = None,
            reply_to: str = None,
            reply_to_prefix: str = None,
            create_reply_topic: bool = None,
            reply_expires: Seconds = None,
            Stream: SymbolArg[Type[StreamT]] = None,
            Table: SymbolArg[Type[TableT]] = None,
            TableManager: SymbolArg[Type[TableManagerT]] = None,
            Set: SymbolArg[Type[SetT]] = None,
            Serializers: SymbolArg[Type[RegistryT]] = None,
            Worker: SymbolArg[Type[WorkerT]] = None,
            PartitionAssignor: SymbolArg[Type[PartitionAssignorT]] = None,
            Router: SymbolArg[Type[RouterT]] = None,
            stream_buffer_maxsize: int = None,
            loop: asyncio.AbstractEventLoop = None,
            loghandlers: List[logging.StreamHandler] = None,
            # XXX backward compat (remove fpr Faust 1.0)
            url: Union[str, URL] = None) -> None:
        self.version = version if version is not None else self._version
        self.id_format = id_format if id_format is not None else self.id_format
        self.loop = loop if loop is not None else self.loop
        self.origin = origin if origin is not None else self.origin
        self.id = id
        self.broker = broker or self._broker or BROKER_URL
        self.store = store or self._store or STORE_URL
        if autodiscover is not None:
            self.autodiscover = autodiscover
        self.client_id = client_id if client_id is not None else self.client_id
        self.canonical_url = canonical_url or self._canonical_url or ''
        # datadir is a format string that can contain {appid}
        self.datadir = datadir or self._datadir or DATADIR
        self.tabledir = tabledir or self._tabledir or TABLEDIR
        self.commit_interval = commit_interval or self._commit_interval
        self.table_cleanup_interval = (
            table_cleanup_interval or self._table_cleanup_interval)

        if key_serializer is not None:
            self.key_serializer = key_serializer
        if value_serializer is not None:
            self.value_serializer = value_serializer
        if num_standby_replicas is not None:
            self.num_standby_replicas = num_standby_replicas
        if replication_factor is not None:
            self.replication_factor = replication_factor
        if default_partitions is not None:
            self.default_partitions = default_partitions
        if create_reply_topic is not None:
            self.create_reply_topic = create_reply_topic
        if loghandlers is not None:
            self.loghandlers = loghandlers
        if stream_buffer_maxsize is not None:
            self.stream_buffer_maxsize = stream_buffer_maxsize

        if reply_to_prefix is not None:
            self.reply_to_prefix = reply_to_prefix
        if reply_to is not None:
            self.reply_to = reply_to
        else:
            self.reply_to = f'{self.reply_to_prefix}{uuid4()}'

        self.Stream = Stream or self._Stream or STREAM_TYPE
        self.Table = Table or self._Table or TABLE_TYPE
        self.Set = Set or self._Set or SET_TYPE
        self.TableManager = (
            TableManager or self._TableManager or TABLE_MANAGER_TYPE)
        self.Serializers = Serializers or self._Serializers or REGISTRY_TYPE
        self.Worker = Worker or self._Worker or WORKER_TYPE
        self.PartitionAssignor = (
            PartitionAssignor or
            self._PartitionAssignor or
            PARTITION_ASSIGNOR_TYPE)
        self.Router = Router or self._Router or ROUTER_TYPE

    def _datadir_path(self, path: Path) -> Path:
        return path if path.is_absolute() else self.datadir / path

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, id: str) -> None:
        if self.version > 1:
            id = self.id_format.format(id=id, self=self)
        self._id = id

    @property
    def version(self) -> int:
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        if not version:
            raise ImproperlyConfigured(
                'Version cannot be {version}, please start at 1')
        self._version = version

    @property
    def broker(self) -> URL:
        return self._broker

    @broker.setter
    def broker(self, broker: Union[URL, str]) -> None:
        self._broker = URL(broker)

    @property
    def store(self) -> URL:
        return self._store

    @store.setter
    def store(self, store: Union[URL, str]) -> None:
        self._store = URL(store)

    @property
    def canonical_url(self) -> URL:
        return self._canonical_url

    @canonical_url.setter
    def canonical_url(self, canonical_url: Union[URL, str]) -> None:
        self._canonical_url = URL(canonical_url)

    @property
    def datadir(self) -> Path:
        return self._datadir

    @datadir.setter
    def datadir(self, datadir: Union[Path, str]) -> None:
        self._datadir = Path(str(datadir).format(appid=self.id)).expanduser()

    @property
    def tabledir(self) -> Path:
        return self._tabledir

    @tabledir.setter
    def tabledir(self, tabledir: Union[Path, str]) -> None:
        self._tabledir = self._datadir_path(Path(tabledir)).expanduser()

    @property
    def commit_interval(self) -> float:
        return self._commit_interval

    @commit_interval.setter
    def commit_interval(self, value: Seconds) -> None:
        self._commit_interval = want_seconds(value)

    @property
    def table_cleanup_interval(self) -> float:
        return self._table_cleanup_interval

    @table_cleanup_interval.setter
    def table_cleanup_interval(self, value: Seconds) -> None:
        self._table_cleanup_interval = want_seconds(value)

    @property
    def reply_expires(self) -> float:
        return self._reply_expires

    @reply_expires.setter
    def reply_expires(self, reply_expires: Seconds) -> None:
        self._reply_expires = want_seconds(reply_expires)

    @property
    def Stream(self) -> Type[StreamT]:
        return self._Stream

    @Stream.setter
    def Stream(self, Stream: SymbolArg[Type[StreamT]]) -> None:
        self._Stream = symbol_by_name(Stream)

    @property
    def Table(self) -> Type[TableT]:
        return self._Table

    @Table.setter
    def Table(self, Table: SymbolArg[Type[TableT]]) -> None:
        self._Table = symbol_by_name(Table)

    @property
    def Set(self) -> Type[SetT]:
        return self._Set

    @Set.setter
    def Set(self, Set: SymbolArg[Type[SetT]]) -> None:
        self._Set = symbol_by_name(Set)

    @property
    def TableManager(self) -> Type[TableManagerT]:
        return self._TableManager

    @TableManager.setter
    def TableManager(self, Manager: SymbolArg[Type[TableManagerT]]) -> None:
        self._TableManager = symbol_by_name(Manager)

    @property
    def Serializers(self) -> Type[RegistryT]:
        return self._Serializers

    @Serializers.setter
    def Serializers(self, Serializers: SymbolArg[Type[RegistryT]]) -> None:
        self._Serializers = symbol_by_name(Serializers)

    @property
    def Worker(self) -> Type[WorkerT]:
        return self._Worker

    @Worker.setter
    def Worker(self, Worker: SymbolArg[Type[WorkerT]]) -> None:
        self._Worker = symbol_by_name(Worker)

    @property
    def PartitionAssignor(self) -> Type[PartitionAssignorT]:
        return self._PartitionAssignor

    @PartitionAssignor.setter
    def PartitionAssignor(
            self, Assignor: SymbolArg[Type[PartitionAssignorT]]) -> None:
        self._PartitionAssignor = symbol_by_name(Assignor)

    @property
    def Router(self) -> Type[RouterT]:
        return self._Router

    @Router.setter
    def Router(self, Router: SymbolArg[Type[RouterT]]) -> None:
        self._Router = symbol_by_name(Router)
