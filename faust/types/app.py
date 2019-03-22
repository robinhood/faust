import abc
import asyncio
import typing
from datetime import tzinfo
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    ClassVar,
    Iterable,
    Mapping,
    MutableSequence,
    Optional,
    Pattern,
    Set,
    Type,
    Union,
    no_type_check,
)

from mode import Seconds, ServiceT, Signal, SupervisorStrategyT, SyncSignal
from mode.utils.futures import stampede
from mode.utils.objects import cached_property
from mode.utils.queues import FlowControlEvent, ThrowableQueue
from mode.utils.types.trees import NodeT
from mode.utils.typing import NoReturn


from .agents import AgentFun, AgentManagerT, AgentT, SinkT
from .assignor import PartitionAssignorT
from .codecs import CodecArg
from .core import HeadersArg, K, V
from .fixups import FixupT
from .router import RouterT
from .sensors import SensorDelegateT
from .serializers import RegistryT
from .streams import StreamT
from .tables import CollectionT, TableManagerT, TableT
from .topics import ChannelT, TopicT
from .transports import ConductorT, ConsumerT, ProducerT, TransportT
from .tuples import MessageSentCallback, RecordMetadata, TP
from .web import (
    CacheBackendT,
    HttpClientT,
    PageArg,
    ResourceOptions,
    View,
    ViewDecorator,
    Web,
)
from .windows import WindowT

if typing.TYPE_CHECKING:
    from faust.cli.base import AppCommand as _AppCommand
    from faust.sensors.monitor import Monitor as _Monitor
    from faust.worker import Worker as _Worker
    from .models import ModelArg as _ModelArg
    from .settings import Settings as _Settings
else:
    class _AppCommand: ...     # noqa
    class _Monitor: ...        # noqa
    class _Worker: ...         # noqa
    class _ModelArg: ...       # noqa
    class _Settings: ...       # noqa

__all__ = [
    'TaskArg',
    'AppT',
]

TaskArg = Union[Callable[['AppT'], Awaitable], Callable[[], Awaitable]]


class BootStrategyT:
    app: 'AppT'

    enable_kafka: bool = True
    # We want these to take default from `enable_kafka`
    # attribute, but still want to allow subclasses to define
    # them like this:
    #   class MyBoot(BootStrategy):
    #       enable_kafka_consumer = False
    enable_kafka_consumer: Optional[bool] = None
    enable_kafka_producer: Optional[bool] = None

    enable_web: Optional[bool] = None
    enable_sensors: bool = True

    @abc.abstractmethod
    def __init__(self, app: 'AppT', *,
                 enable_web: bool = None,
                 enable_kafka: bool = True,
                 enable_kafka_producer: bool = None,
                 enable_kafka_consumer: bool = None,
                 enable_sensors: bool = True) -> None:
        ...

    @abc.abstractmethod
    def server(self) -> Iterable[ServiceT]:
        ...

    @abc.abstractmethod
    def client_only(self) -> Iterable[ServiceT]:
        ...

    @abc.abstractmethod
    def producer_only(self) -> Iterable[ServiceT]:
        ...


class AppT(ServiceT):
    """Abstract type for the Faust application.

    See Also:
        :class:`faust.App`.
    """
    Settings: ClassVar[Type[_Settings]]

    BootStrategy: ClassVar[Type[BootStrategyT]]
    boot_strategy: BootStrategyT

    #: Set to true when the app is finalized (can read configuration).
    finalized: bool = False

    #: Set to true when the app has read configuration.
    configured: bool = False

    #: Set to true if the worker is currently rebalancing.
    rebalancing: bool = False
    rebalancing_count: int = 0

    #: Set to true if the assignment is empty
    # This flag is set by App._on_partitions_assigned
    unassigned: bool = False

    #: Set to true when app is executing within a worker instance.
    # This flag is set in faust/worker.py
    in_worker: bool = False

    on_configured: SyncSignal[_Settings] = SyncSignal()
    on_before_configured: SyncSignal = SyncSignal()
    on_after_configured: SyncSignal = SyncSignal()
    on_partitions_assigned: Signal[Set[TP]] = Signal()
    on_partitions_revoked: Signal[Set[TP]] = Signal()
    on_rebalance_complete: Signal = Signal()
    on_before_shutdown: Signal = Signal()
    on_worker_init: SyncSignal = SyncSignal()

    client_only: bool

    agents: AgentManagerT
    sensors: SensorDelegateT

    fixups: MutableSequence[FixupT]

    @abc.abstractmethod
    def __init__(self,
                 id: str,
                 *,
                 monitor: _Monitor,
                 config_source: Any = None,
                 **options: Any) -> None:
        self.on_startup_finished: Optional[Callable] = None

    @abc.abstractmethod
    def config_from_object(self,
                           obj: Any,
                           *,
                           silent: bool = False,
                           force: bool = False) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> None:
        ...

    @abc.abstractmethod
    def main(self) -> NoReturn:
        ...

    @abc.abstractmethod
    def worker_init(self) -> None:
        ...

    @abc.abstractmethod
    def discover(self,
                 *extra_modules: str,
                 categories: Iterable[str] = ('a', 'b', 'c'),
                 ignore: Iterable[str] = ('foo', 'bar')) -> None:
        ...

    @abc.abstractmethod
    def topic(self,
              *topics: str,
              pattern: Union[str, Pattern] = None,
              key_type: _ModelArg = None,
              value_type: _ModelArg = None,
              key_serializer: CodecArg = None,
              value_serializer: CodecArg = None,
              partitions: int = None,
              retention: Seconds = None,
              compacting: bool = None,
              deleting: bool = None,
              replicas: int = None,
              acks: bool = True,
              internal: bool = False,
              config: Mapping[str, Any] = None,
              maxsize: int = None,
              allow_empty: bool = False,
              loop: asyncio.AbstractEventLoop = None) -> TopicT:
        ...

    @abc.abstractmethod
    def channel(self,
                *,
                key_type: _ModelArg = None,
                value_type: _ModelArg = None,
                maxsize: int = None,
                loop: asyncio.AbstractEventLoop = None) -> ChannelT:
        ...

    @abc.abstractmethod
    def agent(self,
              channel: Union[str, ChannelT] = None,
              *,
              name: str = None,
              concurrency: int = 1,
              supervisor_strategy: Type[SupervisorStrategyT] = None,
              sink: Iterable[SinkT] = None,
              isolated_partitions: bool = False,
              use_reply_headers: bool = False,
              **kwargs: Any) -> Callable[[AgentFun], AgentT]:
        ...

    @abc.abstractmethod
    @no_type_check
    def task(self, fun: TaskArg) -> Callable:
        ...

    @abc.abstractmethod
    def timer(self, interval: Seconds, on_leader: bool = False) -> Callable:
        ...

    @abc.abstractmethod
    def crontab(self, cron_format: str, *,
                timezone: tzinfo = None,
                on_leader: bool = False) -> Callable:
        ...

    @abc.abstractmethod
    def service(self, cls: Type[ServiceT]) -> Type[ServiceT]:
        ...

    @abc.abstractmethod
    def stream(self,
               channel: AsyncIterable,
               beacon: NodeT = None,
               **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    def Table(self,
              name: str,
              *,
              default: Callable[[], Any] = None,
              window: WindowT = None,
              partitions: int = None,
              help: str = None,
              **kwargs: Any) -> TableT:
        ...

    @abc.abstractmethod
    def page(self, path: str, *,
             base: Type[View] = View,
             cors_options: Mapping[str, ResourceOptions] = None,
             name: str = None) -> Callable[[PageArg], Type[View]]:
        ...

    @abc.abstractmethod
    def table_route(self, table: CollectionT,
                    shard_param: str = None,
                    *,
                    query_param: str = None,
                    match_info: str = None) -> ViewDecorator:
        ...

    @abc.abstractmethod
    def command(self,
                *options: Any,
                base: Type[_AppCommand] = None,
                **kwargs: Any) -> Callable[[Callable], Type[_AppCommand]]:
        ...

    @abc.abstractmethod
    async def start_client(self) -> None:
        ...

    @abc.abstractmethod
    async def maybe_start_client(self) -> None:
        ...

    @abc.abstractmethod
    async def send(
            self,
            channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            timestamp: float = None,
            headers: HeadersArg = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        ...

    @stampede
    @abc.abstractmethod
    async def maybe_start_producer(self) -> ProducerT:
        ...

    @abc.abstractmethod
    def is_leader(self) -> bool:
        ...

    @abc.abstractmethod
    def FlowControlQueue(
            self,
            maxsize: int = None,
            *,
            clear_on_resume: bool = False,
            loop: asyncio.AbstractEventLoop = None) -> ThrowableQueue:
        ...

    @abc.abstractmethod
    def Worker(self, **kwargs: Any) -> _Worker:
        ...

    @abc.abstractmethod
    def on_webserver_init(self, web: Web) -> None:
        ...

    @abc.abstractmethod
    def on_rebalance_start(self) -> None:
        ...

    @abc.abstractmethod
    def on_rebalance_end(self) -> None:
        ...

    @property
    def conf(self) -> _Settings:
        ...

    @conf.setter
    def conf(self, settings: _Settings) -> None:
        ...

    @property
    @abc.abstractmethod
    def transport(self) -> TransportT:
        ...

    @transport.setter
    def transport(self, transport: TransportT) -> None:
        ...

    @property
    @abc.abstractmethod
    def cache(self) -> CacheBackendT:
        ...

    @cache.setter
    def cache(self, cache: CacheBackendT) -> None:
        ...

    @property
    @abc.abstractmethod
    def producer(self) -> ProducerT:
        ...

    @property
    @abc.abstractmethod
    def consumer(self) -> ConsumerT:
        ...

    @cached_property
    @abc.abstractmethod
    def tables(self) -> TableManagerT:
        ...

    @cached_property
    @abc.abstractmethod
    def topics(self) -> ConductorT:
        ...

    @property
    @abc.abstractmethod
    def monitor(self) -> _Monitor:
        ...

    @monitor.setter
    def monitor(self, value: _Monitor) -> None:
        ...

    @cached_property
    @abc.abstractmethod
    def flow_control(self) -> FlowControlEvent:
        return FlowControlEvent(loop=self.loop)

    @property
    @abc.abstractmethod
    def http_client(self) -> HttpClientT:
        ...

    @http_client.setter
    def http_client(self, client: HttpClientT) -> None:
        ...

    @property
    @abc.abstractmethod
    def assignor(self) -> PartitionAssignorT:
        ...

    @property
    @abc.abstractmethod
    def router(self) -> RouterT:
        ...

    @property
    @abc.abstractmethod
    def serializers(self) -> RegistryT:
        ...

    @property
    @abc.abstractmethod
    def web(self) -> Web:
        ...

    @web.setter
    def web(self, web: Web) -> None:
        ...

    @property
    @abc.abstractmethod
    def in_transaction(self) -> bool:
        ...
