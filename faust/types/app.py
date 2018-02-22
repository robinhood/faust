import abc
import asyncio
import typing
from typing import (
    Any, AsyncIterable, Awaitable, Callable, Iterable,
    List, Mapping, Pattern, Set, Tuple, Type, Union,
)

from aiohttp.client import ClientSession
from mode import Seconds, ServiceT, Signal, SupervisorStrategyT, SyncSignal
from mode.utils.futures import FlowControlEvent, ThrowableQueue, stampede
from mode.utils.types.trees import NodeT

from .agents import AgentFun, AgentManagerT, AgentT, SinkT
from .assignor import PartitionAssignorT
from .codecs import CodecArg
from .core import K, V
from .router import RouterT
from .sensors import SensorDelegateT
from .serializers import RegistryT
from .streams import StreamT
from .tables import CollectionT, SetT, TableManagerT, TableT
from .topics import ChannelT, ConductorT, TopicT
from .transports import ConsumerT, ProducerT, TransportT
from .tuples import MessageSentCallback, RecordMetadata, TP
from .windows import WindowT

from ..utils.objects import cached_property

if typing.TYPE_CHECKING:
    from ..cli.base import AppCommand
    from ..sensors.monitor import Monitor
    from ..web.base import Request, Response, Web
    from ..web.views import Site, View
    from ..worker import Worker as WorkerT
    from .models import ModelArg
    from .settings import Settings
else:
    class AppCommand: ...     # noqa
    class Monitor: ...        # noqa
    class ModelArg: ...       # noqa
    class Request: ...        # noqa
    class Response: ...       # noqa
    class Site: ...           # noqa
    class View: ...           # noqa
    class WorkerT: ...        # noqa
    class Web: ...            # noqa
    class Settings: ...       # noqa

__all__ = [
    'TaskArg',
    'ViewGetHandler',
    'RoutedViewGetHandler',
    'PageArg',
    'Web',
    'AppT',
]

TaskArg = Union[
    Callable[['AppT'], Awaitable],
    Callable[[], Awaitable],
]
ViewGetHandler = Callable[[View, Request], Awaitable[Response]]
RoutedViewGetHandler = Callable[[ViewGetHandler], ViewGetHandler]
PageArg = Union[Type[View], ViewGetHandler]


class AppT(ServiceT):
    """Abstract type for the Faust application.

    See Also:
        :class:`faust.App`.
    """

    finalized: bool = False
    configured: bool = False

    on_configured: SyncSignal[Settings] = SyncSignal()
    on_before_configured: SyncSignal = SyncSignal()
    on_after_configured: SyncSignal = SyncSignal()
    on_partitions_assigned: Signal[Set[TP]] = Signal()
    on_partitions_revoked: Signal[Set[TP]] = Signal()

    client_only: bool

    agents: AgentManagerT
    sensors: SensorDelegateT
    pages: List[Tuple[str, Type[Site]]]

    @abc.abstractmethod
    def __init__(self, id: str, *,
                 monitor: Monitor,
                 config_source: Any = None,
                 **options: Any) -> None:
        self.on_startup_finished: Callable = None

    @abc.abstractmethod
    def config_from_object(self, obj: Any,
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
    def discover(self,
                 *extra_modules: str,
                 categories: Iterable[str] = None,
                 ignore: Iterable[str] = None) -> None:
        ...

    @abc.abstractmethod
    def topic(self, *topics: str,
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
              config: Mapping[str, Any] = None) -> TopicT:
        ...

    @abc.abstractmethod
    def channel(self, *,
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
              **kwargs: Any) -> Callable[[AgentFun], AgentT]:
        ...

    @abc.abstractmethod
    def task(self, fun: TaskArg) -> Callable:
        ...

    @abc.abstractmethod
    def timer(self, interval: Seconds,
              on_leader: bool = False) -> Callable:
        ...

    @abc.abstractmethod
    def service(self, cls: Type[ServiceT]) -> Type[ServiceT]:
        ...

    @abc.abstractmethod
    def stream(self, channel: AsyncIterable,
               beacon: NodeT = None,
               **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    def Table(self, name: str,
              *,
              default: Callable[[], Any] = None,
              window: WindowT = None,
              partitions: int = None,
              help: str = None,
              **kwargs: Any) -> TableT:
        ...

    @abc.abstractmethod
    def Set(self, name: str,
            *,
            window: WindowT = None,
            partitions: int = None,
            help: str = None,
            **kwargs: Any) -> SetT:
        ...

    @abc.abstractmethod
    def page(self, path: str,
             *,
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
            self, channel: Union[ChannelT, str],
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
    def client_session(self) -> ClientSession:
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
