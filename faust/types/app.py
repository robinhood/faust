import abc
import asyncio
import typing
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Iterable,
    List,
    Mapping,
    MutableSequence,
    Optional,
    Pattern,
    Set,
    Tuple,
    Type,
    Union,
)

from mode import Seconds, ServiceT, Signal, SupervisorStrategyT, SyncSignal
from mode.utils.futures import stampede
from mode.utils.objects import cached_property
from mode.utils.queues import FlowControlEvent, ThrowableQueue
from mode.utils.types.trees import NodeT


from .agents import AgentFun, AgentManagerT, AgentT, SinkT
from .assignor import PartitionAssignorT
from .codecs import CodecArg
from .core import K, V
from .fixups import FixupT
from .router import RouterT
from .sensors import SensorDelegateT
from .serializers import RegistryT
from .streams import StreamT
from .tables import CollectionT, TableManagerT, TableT
from .topics import ChannelT, TopicT
from .transports import ConductorT, ConsumerT, ProducerT, TransportT
from .tuples import MessageSentCallback, RecordMetadata, TP
from .web import HttpClientT, PageArg, RoutedViewGetHandler, Site, View, Web
from .windows import WindowT

if typing.TYPE_CHECKING:
    from faust.cli.base import AppCommand
    from faust.sensors.monitor import Monitor
    from faust.worker import Worker as WorkerT
    from .models import ModelArg
    from .settings import Settings
else:
    class AppCommand: ...     # noqa
    class Monitor: ...        # noqa
    class ModelArg: ...       # noqa
    class WorkerT: ...        # noqa
    class Settings: ...       # noqa

__all__ = [
    'TaskArg',
    'AppT',
]

TaskArg = Union[Callable[['AppT'], Awaitable], Callable[[], Awaitable]]


class AppT(ServiceT):
    """Abstract type for the Faust application.

    See Also:
        :class:`faust.App`.
    """

    #: Set to true when the app is finalized (can read configuration).
    finalized: bool = False

    #: Set to true when the app has read configuration.
    configured: bool = False

    #: Set to true if the worker is currently rebalancing.
    rebalancing: bool = False

    #: Set to true if the assignment is empty
    # This flag is set by App._on_partitions_assigned
    unassigned: bool = False

    on_configured: SyncSignal[Settings] = SyncSignal()
    on_before_configured: SyncSignal = SyncSignal()
    on_after_configured: SyncSignal = SyncSignal()
    on_partitions_assigned: Signal[Set[TP]] = Signal()
    on_partitions_revoked: Signal[Set[TP]] = Signal()
    on_worker_init: SyncSignal = SyncSignal()

    client_only: bool

    agents: AgentManagerT
    sensors: SensorDelegateT
    pages: List[Tuple[str, Type[Site]]]

    fixups: MutableSequence[FixupT]

    @abc.abstractmethod
    def __init__(self,
                 id: str,
                 *,
                 monitor: Monitor,
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
    def main(self) -> None:
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
              key_type: ModelArg = None,
              value_type: ModelArg = None,
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
              loop: asyncio.AbstractEventLoop = None) -> TopicT:
        ...

    @abc.abstractmethod
    def channel(self,
                *,
                key_type: ModelArg = None,
                value_type: ModelArg = None,
                maxsize: int = 1,
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
              **kwargs: Any) -> Callable[[AgentFun], AgentT]:
        ...

    @abc.abstractmethod
    def task(self, fun: TaskArg) -> Callable:
        ...

    @abc.abstractmethod
    def timer(self, interval: Seconds, on_leader: bool = False) -> Callable:
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
             base: Type[View] = View) -> Callable[[PageArg], Type[Site]]:
        ...

    @abc.abstractmethod
    def table_route(self, table: CollectionT,
                    shard_param: str) -> RoutedViewGetHandler:
        ...

    @abc.abstractmethod
    def command(self,
                *options: Any,
                base: Type[AppCommand] = None,
                **kwargs: Any) -> Callable[[Callable], Type[AppCommand]]:
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
    def Worker(self, **kwargs: Any) -> WorkerT:
        ...

    @abc.abstractmethod
    def on_webserver_init(self, web: Web) -> None:
        ...

    @property
    def conf(self) -> Settings:
        ...

    @conf.setter
    def conf(self, settings: Settings) -> None:
        ...

    @property
    @abc.abstractmethod
    def transport(self) -> TransportT:
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
    def monitor(self) -> Monitor:
        ...

    @monitor.setter
    def monitor(self, value: Monitor) -> None:
        ...

    @cached_property
    @abc.abstractmethod
    def flow_control(self) -> FlowControlEvent:
        return FlowControlEvent(loop=self.loop)

    @property
    @abc.abstractmethod
    def http_client(self) -> HttpClientT:
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
