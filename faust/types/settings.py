import abc
import inspect
import logging
import typing
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, List, Optional, Set, Type, Union
from uuid import uuid4

from mode import Seconds, SupervisorStrategyT, want_seconds
from mode.utils.imports import SymbolArg, symbol_by_name
from mode.utils.logging import Severity
from yarl import URL

from faust.cli._env import DATADIR
from faust.exceptions import ImproperlyConfigured

from .agents import AgentT
from .assignor import LeaderAssignorT, PartitionAssignorT
from .codecs import CodecArg
from .router import RouterT
from .sensors import SensorT
from .serializers import RegistryT
from .streams import StreamT
from .tables import TableManagerT, TableT
from .topics import TopicT
from .web import HttpClientT

if typing.TYPE_CHECKING:
    from .worker import Worker as WorkerT
else:
    class WorkerT: ...      # noqa

__all__ = ['AutodiscoverArg', 'Settings']

# XXX mypy borks if we do `from faust import __version__`
faust_version: str = symbol_by_name('faust:__version__')

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

#: Default agent supervisor type, used as default for
#: :setting:`agent_supervisor`.
AGENT_SUPERVISOR_TYPE = 'mode.OneForOneSupervisor'

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
ROUTER_TYPE = 'faust.app.router:Router'

#: Path to topic class, providing the default for :setting:`Topic`.
TOPIC_TYPE = 'faust:Topic'

#: Path to HTTP client class, providing the default for :setting:`HttpClient`.
HTTP_CLIENT_TYPE = 'faust.types.web.HttpClientT'

#: Path to Monitor sensor class, providing the default for :setting:`Monitor`.
MONITOR_TYPE = 'faust.sensors:Monitor'

#: Default Kafka Client ID.
BROKER_CLIENT_ID = f'faust-{faust_version}'

#: How often we commit acknowledged messages: every n messages.
#: Used as the default value for :setting:`broker_commit_every`.
BROKER_COMMIT_EVERY = 10_000

#: How often we commit acknowledged messages on a timer.
#: Used as the default value for :setting:`broker_commit_interval`.
BROKER_COMMIT_INTERVAL = 2.8

#: Kafka consumer session timeout (``session_timeout_ms``).
BROKER_SESSION_TIMEOUT = 30.0

#: Kafka consumer heartbeat (``heartbeat_interval_ms``).
BROKER_HEARTBEAT_INTERVAL = 3.0

#: How long time it takes before we warn that the commit offset has
#: not advanced.
BROKER_LIVELOCK_SOFT = want_seconds(timedelta(minutes=5))

#: How often we clean up expired items in windowed tables.
#: Used as the default value for :setting:`table_cleanup_interval`.
TABLE_CLEANUP_INTERVAL = 30.0

#: Prefix used for reply topics.
REPLY_TO_PREFIX = 'f-reply-'

#: Default expiry time for replies, in seconds (float).
REPLY_EXPIRES = want_seconds(timedelta(days=1))

#: Max number of messages channels/streams/topics can "prefetch".
STREAM_BUFFER_MAXSIZE = 4096

#: We buffer up sending messages until the
#: source topic offset related to that processsing is committed.
#: This means when we do commit, we may have buffered up a LOT of messages
#: so commit frequently.
#:
#: This setting is deprecated and will be removed once transaction support
#: is added in a later version.
STREAM_PUBLISH_ON_COMMIT = True

#: Minimum time to batch before sending out messages from the producer.
#: Used as the default value for :setting:`linger_ms`.
PRODUCER_LINGER_MS = 0

#: Maximum size of buffered data per partition in bytes in the producer.
#: Used as the default value for :setting:`max_batch_size`.
PRODUCER_MAX_BATCH_SIZE = 16_384

#: Maximum size of a request in bytes in the producer.
#: Used as the default value for :setting:`max_request_size`.
PRODUCER_MAX_REQUEST_SIZE = 1_000_000

#: The compression type for all data generated by
#: the producer. Valid values are 'gzip', 'snappy', 'lz4', or None.
#: Compression is of full batches of data, so the efficacy of batching
#: will also impact the compression ratio (more batching means better
#: compression). Default: None.
PRODUCER_COMPRESSION_TYPE: Optional[str] = None

#: The number of acknowledgments the producer requires the leader to have
#: received before considering a request complete. This controls the
#: durability of records that are sent. The following settings are common:
#:     0: Producer will not wait for any acknowledgment from the server
#:         at all. The message will immediately be considered sent.
#:         (Not recommended)
#:     1: The broker leader will write the record to its local log but
#:         will respond without awaiting full acknowledgement from all
#:         followers. In this case should the leader fail immediately
#:         after acknowledging the record but before the followers have
#:         replicated it then the record will be lost.
#:     -1: The broker leader will wait for the full set of in-sync
#:         replicas to acknowledge the record. This guarantees that the
#:         record will not be lost as long as at least one in-sync replica
#:         remains alive. This is the strongest available guarantee.
PRODUCER_ACKS = -1

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
    broker_commit_every: int = BROKER_COMMIT_EVERY
    broker_check_crcs: bool = True
    id_format: str = '{id}-v{self.version}'
    origin: Optional[str] = None
    key_serializer: CodecArg = 'json'
    value_serializer: CodecArg = 'json'
    reply_to: str
    reply_to_prefix: str = REPLY_TO_PREFIX
    reply_create_topic: bool = False
    stream_buffer_maxsize: int = STREAM_BUFFER_MAXSIZE
    stream_wait_empty: bool = False
    stream_ack_cancelled_tasks: bool = False
    stream_ack_exceptions: bool = True
    stream_publish_on_commit: bool = STREAM_PUBLISH_ON_COMMIT
    table_standby_replicas: int = 1
    topic_replication_factor: int = 1
    topic_partitions: int = 8  # noqa: E704
    loghandlers: List[logging.StreamHandler]
    producer_linger_ms: int = PRODUCER_LINGER_MS
    producer_max_batch_size: int = PRODUCER_MAX_BATCH_SIZE
    producer_acks: int = PRODUCER_ACKS
    producer_max_request_size: int = PRODUCER_MAX_REQUEST_SIZE
    producer_compression_type: Optional[str] = PRODUCER_COMPRESSION_TYPE
    worker_redirect_stdouts: bool = True
    worker_redirect_stdouts_level: Severity = 'WARN'

    _id: str
    _name: str
    _version: int = 1
    _broker: URL
    _store: URL
    _canonical_url: URL
    _datadir: Path
    _tabledir: Path
    _agent_supervisor: Type[SupervisorStrategyT]
    _broker_session_timeout: float = BROKER_SESSION_TIMEOUT
    _broker_heartbeat_interval: float = BROKER_HEARTBEAT_INTERVAL
    _broker_commit_interval: float = BROKER_COMMIT_INTERVAL
    _broker_commit_livelock_soft_timeout: float = BROKER_LIVELOCK_SOFT
    _table_cleanup_interval: float = TABLE_CLEANUP_INTERVAL
    _reply_expires: float = REPLY_EXPIRES
    _Agent: Type[AgentT]
    _Stream: Type[StreamT]
    _Table: Type[TableT]
    _TableManager: Type[TableManagerT]
    _Serializers: Type[RegistryT]
    _Worker: Type[WorkerT]
    _PartitionAssignor: Type[PartitionAssignorT]
    _LeaderAssignor: Type[LeaderAssignorT]
    _Router: Type[RouterT]
    _Topic: Type[TopicT]
    _HttpClient: Type[HttpClientT]
    _Monitor: Type[SensorT]

    @classmethod
    def setting_names(cls) -> Set[str]:
        return {
            k for k, v in inspect.signature(cls).parameters.items()
            if v.kind not in SETTINGS_SKIP
        } - SETTINGS_COMPAT

    def __init__(  # noqa: C901
            self,
            id: str,
            *,
            version: int = None,
            broker: Union[str, URL] = None,
            broker_client_id: str = None,
            broker_commit_every: int = None,
            broker_commit_interval: Seconds = None,
            broker_commit_livelock_soft_timeout: Seconds = None,
            broker_session_timeout: Seconds = None,
            broker_heartbeat_interval: Seconds = None,
            broker_check_crcs: bool = None,
            agent_supervisor: SymbolArg[Type[SupervisorStrategyT]] = None,
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
            stream_wait_empty: bool = None,
            stream_ack_cancelled_tasks: bool = None,
            stream_ack_exceptions: bool = None,
            stream_publish_on_commit: bool = None,
            producer_linger_ms: int = None,
            producer_max_batch_size: int = None,
            producer_acks: int = None,
            producer_max_request_size: int = None,
            producer_compression_type: str = None,
            worker_redirect_stdouts: bool = None,
            worker_redirect_stdouts_level: Severity = None,
            Agent: SymbolArg[Type[AgentT]] = None,
            Stream: SymbolArg[Type[StreamT]] = None,
            Table: SymbolArg[Type[TableT]] = None,
            TableManager: SymbolArg[Type[TableManagerT]] = None,
            Serializers: SymbolArg[Type[RegistryT]] = None,
            Worker: SymbolArg[Type[WorkerT]] = None,
            PartitionAssignor: SymbolArg[Type[PartitionAssignorT]] = None,
            LeaderAssignor: SymbolArg[Type[LeaderAssignorT]] = None,
            Router: SymbolArg[Type[RouterT]] = None,
            Topic: SymbolArg[Type[TopicT]] = None,
            HttpClient: SymbolArg[Type[HttpClientT]] = None,
            Monitor: SymbolArg[Type[SensorT]] = None,
            # XXX backward compat (remove for Faust 1.0)
            url: Union[str, URL] = None,
            **kwargs: Any) -> None:
        self.version = version if version is not None else self._version
        self.id_format = id_format if id_format is not None else self.id_format
        self.origin = origin if origin is not None else self.origin
        self.id = id
        self.broker = url or broker or BROKER_URL
        self.store = store or STORE_URL
        if autodiscover is not None:
            self.autodiscover = autodiscover
        if broker_client_id is not None:
            self.broker_client_id = broker_client_id
        self.canonical_url = canonical_url or ''
        # datadir is a format string that can contain e.g. {conf.id}
        self.datadir = datadir or DATADIR
        self.tabledir = tabledir or TABLEDIR
        self.broker_commit_interval = (
            broker_commit_interval or self._broker_commit_interval)
        self.broker_commit_livelock_soft_timeout = (
            broker_commit_livelock_soft_timeout or
            self._broker_commit_livelock_soft_timeout)
        if broker_session_timeout is not None:
            self.broker_session_timeout = want_seconds(broker_session_timeout)
        if broker_heartbeat_interval is not None:
            self.broker_heartbeat_interval = want_seconds(
                broker_heartbeat_interval)
        self.table_cleanup_interval = (
            table_cleanup_interval or self._table_cleanup_interval)

        if broker_commit_every is not None:
            self.broker_commit_every = broker_commit_every
        if broker_check_crcs is not None:
            self.broker_check_crcs = broker_check_crcs
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
        self.loghandlers = loghandlers if loghandlers is not None else []
        if stream_buffer_maxsize is not None:
            self.stream_buffer_maxsize = stream_buffer_maxsize
        if stream_wait_empty is not None:
            self.stream_wait_empty = stream_wait_empty
        if stream_ack_cancelled_tasks is not None:
            self.stream_ack_cancelled_tasks = stream_ack_cancelled_tasks
        if stream_ack_exceptions is not None:
            self.stream_ack_exceptions = stream_ack_exceptions
        if stream_publish_on_commit is not None:
            self.stream_publish_on_commit = stream_publish_on_commit
        if producer_linger_ms is not None:
            self.producer_linger_ms = producer_linger_ms
        if producer_max_batch_size is not None:
            self.producer_max_batch_size = producer_max_batch_size
        if producer_acks is not None:
            self.producer_acks = producer_acks
        if producer_max_request_size is not None:
            self.producer_max_request_size = producer_max_request_size
        if producer_compression_type is not None:
            self.producer_compression_type = producer_compression_type
        if worker_redirect_stdouts is not None:
            self.worker_redirect_stdouts = worker_redirect_stdouts
        if worker_redirect_stdouts_level is not None:
            self.worker_redirect_stdouts_level = worker_redirect_stdouts_level

        if reply_to_prefix is not None:
            self.reply_to_prefix = reply_to_prefix
        if reply_to is not None:
            self.reply_to = reply_to
        else:
            self.reply_to = f'{self.reply_to_prefix}{uuid4()}'
        if reply_expires is not None:
            self.reply_expires = reply_expires

        self.agent_supervisor = agent_supervisor or AGENT_SUPERVISOR_TYPE

        self.Agent = Agent or AGENT_TYPE
        self.Stream = Stream or STREAM_TYPE
        self.Table = Table or TABLE_TYPE
        self.Set = Set or SET_TYPE
        self.TableManager = TableManager or TABLE_MANAGER_TYPE
        self.Serializers = Serializers or REGISTRY_TYPE
        self.Worker = Worker or WORKER_TYPE
        self.PartitionAssignor = PartitionAssignor or PARTITION_ASSIGNOR_TYPE
        self.LeaderAssignor = LeaderAssignor or LEADER_ASSIGNOR_TYPE
        self.Router = Router or ROUTER_TYPE
        self.Topic = Topic or TOPIC_TYPE
        self.HttpClient = HttpClient or HTTP_CLIENT_TYPE
        self.Monitor = Monitor or MONITOR_TYPE
        self.__dict__.update(kwargs)  # arbitrary configuration

    def prepare_id(self, id: str) -> str:
        if self.version > 1:
            return self.id_format.format(id=id, self=self)
        return id

    def prepare_datadir(self, datadir: Union[str, Path]) -> Path:
        return self._Path(str(datadir).format(conf=self))

    def prepare_tabledir(self, tabledir: Union[str, Path]) -> Path:
        return self._appdir_path(self._Path(tabledir))

    def _Path(self, *parts: Union[str, Path]) -> Path:
        return Path(*parts).expanduser()

    def _appdir_path(self, path: Path) -> Path:
        return path if path.is_absolute() else self.appdir / path

    @property
    def name(self) -> str:
        # name is a read-only property
        return self._name

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, name: str) -> None:
        self._name = name
        self._id = self.prepare_id(name)  # id is name+version

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
        self._canonical_url = URL(canonical_url)

    @property
    def datadir(self) -> Path:
        return self._datadir

    @datadir.setter
    def datadir(self, datadir: Union[Path, str]) -> None:
        self._datadir = self.prepare_datadir(datadir)

    @property
    def appdir(self) -> Path:
        return self.datadir / f'v{self.version}'

    @property
    def tabledir(self) -> Path:
        return self._tabledir

    @tabledir.setter
    def tabledir(self, tabledir: Union[Path, str]) -> None:
        self._tabledir = self.prepare_tabledir(tabledir)

    @property
    def broker_session_timeout(self) -> float:
        return self._broker_session_timeout

    @broker_session_timeout.setter
    def broker_session_timeout(self, value: Seconds) -> None:
        self._broker_session_timeout = want_seconds(value)

    @property
    def broker_heartbeat_interval(self) -> float:
        return self._broker_heartbeat_interval

    @broker_heartbeat_interval.setter
    def broker_heartbeat_interval(self, value: Seconds) -> None:
        self._broker_heartbeat_interval = want_seconds(value)

    @property
    def broker_commit_interval(self) -> float:
        return self._broker_commit_interval

    @broker_commit_interval.setter
    def broker_commit_interval(self, value: Seconds) -> None:
        self._broker_commit_interval = want_seconds(value)

    @property
    def broker_commit_livelock_soft_timeout(self) -> float:
        return self._broker_commit_livelock_soft_timeout

    @broker_commit_livelock_soft_timeout.setter
    def broker_commit_livelock_soft_timeout(self, value: Seconds) -> None:
        self._broker_commit_livelock_soft_timeout = want_seconds(value)

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
    def agent_supervisor(self) -> Type[SupervisorStrategyT]:
        return self._agent_supervisor

    @agent_supervisor.setter
    def agent_supervisor(
            self, sup: SymbolArg[Type[SupervisorStrategyT]]) -> None:
        self._agent_supervisor = symbol_by_name(sup)

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
