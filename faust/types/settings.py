import abc
import inspect
import logging
import typing
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, List, Set, Type, Union
from uuid import uuid4

from mode import Seconds, want_seconds
from mode.utils.imports import SymbolArg, symbol_by_name
from yarl import URL

from .. import __version__ as faust_version
from ..cli._env import DATADIR
from ..exceptions import ImproperlyConfigured
from ..types import CodecArg
from ..types.agents import AgentT
from ..types.app import HttpClientT
from ..types.assignor import LeaderAssignorT, PartitionAssignorT
from ..types.router import RouterT
from ..types.sensors import SensorT
from ..types.serializers import RegistryT
from ..types.streams import StreamT
from ..types.tables import SetT, TableManagerT, TableT
from ..types.topics import ConductorT, TopicT

if typing.TYPE_CHECKING:
    from .worker import Worker as WorkerT
else:
    class WorkerT: ...     # noqa

__all__ = ['AutodiscoverArg', 'Settings']

#: Broker URL, used as default for :setting:`broker`.
BROKER_URL = 'kafka://localhost:9092'

#: Table storage URL, used as default for :setting:`store`.
STORE_URL = 'memory://'

#: Table state directory path used as default for :setting:`tabledir`.
#: This path will be treated as relative to datadir, unless the provided
#: poth is absolute.
TABLEDIR = 'tables'

#: Path to agent class, used as default for :setting:`Agent`.
AGENT_TYPE = 'faust.Agent'

#: Path to stream class, used as default for :setting:`Stream`.
STREAM_TYPE = 'faust.Stream'

#: Path to table manager class, used as default for :setting:`TableManager`.
TABLE_MANAGER_TYPE = 'faust.tables.TableManager'

#: Path to table class, used as default for :setting:`Table`.
TABLE_TYPE = 'faust.Table'

#: Path to set class, used as default for :setting:`Set`.
SET_TYPE = 'faust.Set'

#: Path to serializer registry class, used as the default for
#: :setting:`Serializers`.
REGISTRY_TYPE = 'faust.serializers.Registry'

#: Path to worker class, providing the default for :setting:`Worker`.
WORKER_TYPE = 'faust.worker.Worker'

#: Path to partition assignor class, providing the default for
#: :setting:`PartitionAssignor`.
PARTITION_ASSIGNOR_TYPE = 'faust.assignor:PartitionAssignor'

#: Path to leader assignor class, providing the default for
#: :setting:`LeaderAssignor`.
LEADER_ASSIGNOR_TYPE = 'faust.assignor:LeaderAssignor'

#: Path to router class, providing the default for :setting:`Router`.
ROUTER_TYPE = 'faust.router:Router'

#: Path to topic conductor class, providing the default
#: for :setting:`TopicConductor`.
CONDUCTOR_TYPE = 'faust.topics:TopicConductor'

#: Path to topic class, providing the default for :setting:`Topic`.
TOPIC_TYPE = 'faust:Topic'

#: Path to HTTP client class, providing the default for :setting:`HttpClient`.
HTTP_CLIENT_TYPE = 'aiohttp.client:ClientSession'

#: Path to Monitor sensor class, providing the default for :setting:`Monitor`.
MONITOR_TYPE = 'faust.sensors:Monitor'

#: Default Kafka Client ID.
BROKER_CLIENT_ID = f'faust-{faust_version}'

#: How often we commit acknowledged messages.
#: Used as the default value for :setting:`broker_commit_interval`.
BROKER_COMMIT_INTERVAL = 3.0

#: How often we clean up expired items in windowed tables.
#: Used as the default value for :setting:`table_cleanup_interval`.
TABLE_CLEANUP_INTERVAL = 30.0

#: Prefix used for reply topics.
REPLY_TO_PREFIX = 'f-reply-'

#: Default expiry time for replies, in seconds (float).
REPLY_EXPIRES = want_seconds(timedelta(days=1))

#: Max number of messages channels/streams/topics can "prefetch".
STREAM_BUFFER_MAXSIZE = 1000

#: Set of settings added for backwards compatibility
SETTINGS_COMPAT: Set[str] = {'url'}

#: Set of :func:`inspect.signature` parameter types to ignore
#: when building the list of supported seting names.
SETTINGS_SKIP: Set[inspect._ParameterKind] = {
    inspect.Parameter.VAR_KEYWORD,
}

AutodiscoverArg = Union[
    bool,
    Iterable[str],
    Callable[[], Iterable[str]],
]


class Settings(abc.ABC):
    autodiscover: AutodiscoverArg = False
    broker_client_id: str = BROKER_CLIENT_ID
    id_format: str = '{id}-v{self.version}'
    origin: str = None
    key_serializer: CodecArg = 'json'
    value_serializer: CodecArg = 'json'
    reply_to: str = None
    reply_to_prefix: str = REPLY_TO_PREFIX
    reply_create_topic: bool = False
    stream_buffer_maxsize: int = STREAM_BUFFER_MAXSIZE
    table_standby_replicas: int = 1
    topic_replication_factor: int = 1
    topic_partitions: int = 8  # noqa: E704
    loghandlers: List[logging.StreamHandler] = None

    _id: str = None
    _version: int = 1
    _broker: URL = None
    _store: URL = None
    _canonical_url: URL = None
    _datadir: Path = None
    _tabledir: Path = None
    _broker_commit_interval: float = BROKER_COMMIT_INTERVAL
    _table_cleanup_interval: float = TABLE_CLEANUP_INTERVAL
    _reply_expires: float = REPLY_EXPIRES
    _Agent: Type[AgentT] = None
    _Stream: Type[StreamT] = None
    _Table: Type[TableT] = None
    _TableManager: Type[TableManagerT] = None
    _Set: Type[SetT] = None
    _Serializers: Type[RegistryT] = None
    _Worker: Type[WorkerT] = None
    _PartitionAssignor: Type[PartitionAssignorT] = None
    _LeaderAssignor: Type[LeaderAssignorT] = None
    _Router: Type[RouterT] = None
    _TopicConductor: Type[ConductorT] = None
    _Topic: Type[TopicT] = None
    _HttpClient: Type[HttpClientT] = None
    _Monitor: Type[SensorT] = None

    @classmethod
    def setting_names(cls) -> Set[str]:
        return {
            k for k, v in inspect.signature(cls).parameters.items()
            if v.kind not in SETTINGS_SKIP
        } - SETTINGS_COMPAT

    def __init__(
            self, id: str,
            *,
            version: int = None,
            broker: Union[str, URL] = None,
            broker_client_id: str = None,
            broker_commit_interval: Seconds = None,
            store: Union[str, URL] = None,
            autodiscover: AutodiscoverArg = None,
            origin: str = None,
            canonical_url: Union[str, URL] = None,
            datadir: Union[Path, str] = None,
            tabledir: Union[Path, str] = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            loghandlers: List[logging.StreamHandler] = None,
            table_cleanup_interval: Seconds = None,
            table_standby_replicas: int = None,
            topic_replication_factor: int = None,
            topic_partitions: int = None,
            id_format: str = None,
            reply_to: str = None,
            reply_to_prefix: str = None,
            reply_create_topic: bool = None,
            reply_expires: Seconds = None,
            stream_buffer_maxsize: int = None,
            Agent: SymbolArg[Type[AgentT]] = None,
            Stream: SymbolArg[Type[StreamT]] = None,
            Table: SymbolArg[Type[TableT]] = None,
            TableManager: SymbolArg[Type[TableManagerT]] = None,
            Set: SymbolArg[Type[SetT]] = None,
            Serializers: SymbolArg[Type[RegistryT]] = None,
            Worker: SymbolArg[Type[WorkerT]] = None,
            PartitionAssignor: SymbolArg[Type[PartitionAssignorT]] = None,
            LeaderAssignor: SymbolArg[Type[LeaderAssignorT]] = None,
            Router: SymbolArg[Type[RouterT]] = None,
            TopicConductor: SymbolArg[Type[ConductorT]] = None,
            Topic: SymbolArg[Type[TopicT]] = None,
            HttpClient: SymbolArg[Type[HttpClientT]] = None,
            Monitor: SymbolArg[Type[SensorT]] = None,
            # XXX backward compat (remove fpr Faust 1.0)
            url: Union[str, URL] = None,
            **kwargs: Any) -> None:
        self.version = version if version is not None else self._version
        self.id_format = id_format if id_format is not None else self.id_format
        self.origin = origin if origin is not None else self.origin
        self.id = id
        self.broker = broker or self._broker or BROKER_URL
        self.store = store or self._store or STORE_URL
        if autodiscover is not None:
            self.autodiscover = autodiscover
        if broker_client_id is not None:
            self.broker_client_id = broker_client_id
        self.canonical_url = canonical_url or self._canonical_url or ''
        # datadir is a format string that can contain {appid}
        self.datadir = datadir or self._datadir or DATADIR
        self.tabledir = tabledir or self._tabledir or TABLEDIR
        self.broker_commit_interval = (
            broker_commit_interval or self._broker_commit_interval)
        self.table_cleanup_interval = (
            table_cleanup_interval or self._table_cleanup_interval)

        if key_serializer is not None:
            self.key_serializer = key_serializer
        if value_serializer is not None:
            self.value_serializer = value_serializer
        if table_standby_replicas is not None:
            self.table_standby_replicas = table_standby_replicas
        if topic_replication_factor is not None:
            self.topic_replication_factor = topic_replication_factor
        if topic_partitions is not None:
            self.topic_partitions = topic_partitions
        if reply_create_topic is not None:
            self.reply_create_topic = reply_create_topic
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
        if reply_expires is not None:
            self.reply_expires = reply_expires

        self.Agent = Agent or self._Agent or AGENT_TYPE
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
        self.LeaderAssignor = (
            LeaderAssignor or
            self._LeaderAssignor or
            LEADER_ASSIGNOR_TYPE)
        self.Router = Router or self._Router or ROUTER_TYPE
        self.TopicConductor = (
            TopicConductor or
            self._TopicConductor or
            CONDUCTOR_TYPE)
        self.Topic = Topic or self._Topic or TOPIC_TYPE
        self.HttpClient = HttpClient or self._HttpClient or HTTP_CLIENT_TYPE
        self.Monitor = Monitor or self._Monitor or MONITOR_TYPE
        self.__dict__.update(kwargs)  # arbitrary configuration

    def prepare_id(self, id: str) -> str:
        if self.version > 1:
            return self.id_format.format(id=id, self=self)
        return id

    def prepare_datadir(self, datadir: Union[str, Path]) -> Path:
        return Path(str(datadir).format(appid=self.id)).expanduser()

    def prepare_tabledir(self, tabledir: Union[str, Path]) -> Path:
        return self._datadir_path(Path(tabledir)).expanduser()

    def _datadir_path(self, path: Path) -> Path:
        return path if path.is_absolute() else self.datadir / path

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, id: str) -> None:
        self._id = self.prepare_id(id)

    @property
    def version(self) -> int:
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        if not version:
            raise ImproperlyConfigured(
                f'Version cannot be {version}, please start at 1')
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
        self._canonical_url = URL(canonical_url) if canonical_url else None

    @property
    def datadir(self) -> Path:
        return self._datadir

    @datadir.setter
    def datadir(self, datadir: Union[Path, str]) -> None:
        self._datadir = self.prepare_datadir(datadir)

    @property
    def tabledir(self) -> Path:
        return self._tabledir

    @tabledir.setter
    def tabledir(self, tabledir: Union[Path, str]) -> None:
        self._tabledir = self.prepare_tabledir(tabledir)

    @property
    def broker_commit_interval(self) -> float:
        return self._broker_commit_interval

    @broker_commit_interval.setter
    def broker_commit_interval(self, value: Seconds) -> None:
        self._broker_commit_interval = want_seconds(value)

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
    def Agent(self) -> Type[AgentT]:
        return self._Agent

    @Agent.setter
    def Agent(self, Agent: SymbolArg[Type[AgentT]]) -> None:
        self._Agent = symbol_by_name(Agent)

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
    def LeaderAssignor(self) -> Type[LeaderAssignorT]:
        return self._LeaderAssignor

    @LeaderAssignor.setter
    def LeaderAssignor(
            self, Assignor: SymbolArg[Type[LeaderAssignorT]]) -> None:
        self._LeaderAssignor = symbol_by_name(Assignor)

    @property
    def Router(self) -> Type[RouterT]:
        return self._Router

    @Router.setter
    def Router(self, Router: SymbolArg[Type[RouterT]]) -> None:
        self._Router = symbol_by_name(Router)

    @property
    def TopicConductor(self) -> Type[ConductorT]:
        return self._TopicConductor

    @TopicConductor.setter
    def TopicConductor(self, Conductor: SymbolArg[Type[ConductorT]]) -> None:
        self._TopicConductor = symbol_by_name(Conductor)

    @property
    def Topic(self) -> Type[TopicT]:
        return self._Topic

    @Topic.setter
    def Topic(self, Topic: SymbolArg[Type[TopicT]]) -> None:
        self._Topic = symbol_by_name(Topic)

    @property
    def HttpClient(self) -> Type[HttpClientT]:
        return self._HttpClient

    @HttpClient.setter
    def HttpClient(self, HttpClient: SymbolArg[Type[HttpClientT]]) -> None:
        self._HttpClient = symbol_by_name(HttpClient)

    @property
    def Monitor(self) -> Type[SensorT]:
        return self._Monitor

    @Monitor.setter
    def Monitor(self, Monitor: SymbolArg[Type[SensorT]]) -> None:
        self._Monitor = symbol_by_name(Monitor)
