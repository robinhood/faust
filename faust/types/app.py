import abc
import asyncio
import typing

from pathlib import Path
from typing import (
    Any, AsyncIterable, Awaitable, Callable, Iterable, List,
    Mapping, MutableMapping, Pattern, Tuple, Type, Union,
)

from aiohttp.client import ClientSession
from mode import Seconds, ServiceT, SupervisorStrategyT
from mode.utils.types.trees import NodeT
from yarl import URL

from .agents import AgentFun, AgentT, SinkT
from .assignor import PartitionAssignorT
from .codecs import CodecArg
from .core import K, V
from .router import RouterT
from .sensors import SensorDelegateT
from .serializers import RegistryT
from .streams import StreamT
from .tables import SetT, TableManagerT, TableT
from .topics import ChannelT, ConductorT, TopicT
from .transports import ConsumerT, ProducerT, TransportT
from .tuples import MessageSentCallback, RecordMetadata
from .windows import WindowT

from ..utils.futures import FlowControlEvent, stampede
from ..utils.imports import SymbolArg
from ..utils.objects import cached_property

if typing.TYPE_CHECKING:
    from ..cli.base import AppCommand
    from ..sensors.monitor import Monitor
    from ..web.base import Request, Response, Web
    from ..web.views import Site, View
    from ..worker import Worker as WorkerT
    from .models import ModelArg
else:
    class AppCommand: ...     # noqa
    class Monitor: ...        # noqa
    class ModelArg: ...       # noqa
    class Request: ...        # noqa
    class Response: ...       # noqa
    class Web: ...            # noqa
    class Site: ...           # noqa
    class View: ...           # noqa
    class WorkerT: ...        # noqa

__all__ = [
    'TaskArg',
    'ViewGetHandler',
    'PageArg',
    'AutodiscoverArg',
    'AppT',
]


TaskArg = Union[
    Callable[['AppT'], Awaitable],
    Callable[[], Awaitable],
]
ViewGetHandler = Callable[[Web, Request], Awaitable[Response]]
RoutedViewGetHandler = Callable[[ViewGetHandler], ViewGetHandler]
PageArg = Union[Type[View], ViewGetHandler]
AutodiscoverArg = Union[
    bool,
    Iterable[str],
    Callable[[], Iterable[str]],
]


class AppT(ServiceT):
    """Abstract type for the Faust application.

    See Also:
        :class:`faust.App`.
    """

    Stream: Type[StreamT]
    TableType: Type[TableT]
    TableManager: Type[TableManagerT]
    SetType: Type[SetT]
    Serializers: Type[RegistryT]

    id: str
    url: URL
    client_id: str
    datadir: Path
    tabledir: Path
    client_only: bool
    commit_interval: float
    table_cleanup_interval: float
    key_serializer: CodecArg
    value_serializer: CodecArg
    num_standby_replicas: int
    replication_factor: int
    default_partitions: int  # noqa: E704
    reply_to: str
    create_reply_topic: float
    reply_expires: float
    avro_registry_url: URL
    store: URL
    assignor: PartitionAssignorT
    router: RouterT
    canonical_url: URL
    origin: str
    autodiscover: AutodiscoverArg
    stream_buffer_maxsize: int

    agents: MutableMapping[str, AgentT]
    sensors: SensorDelegateT
    serializers: RegistryT
    pages: List[Tuple[str, Type[Site]]]

    @abc.abstractmethod
    def main(self) -> None:
        ...

    @abc.abstractmethod
    def __init__(
            self, id: str,
            *,
            url: Union[str, URL] = 'aiokafka://localhost:9092',
            store: Union[str, URL] = 'memory://',
            autodiscover: AutodiscoverArg = False,
            origin: str = None,
            avro_registry_url: Union[str, URL] = None,
            canonical_url: Union[str, URL] = None,
            client_id: str = '',
            datadir: Union[Path, str] = None,
            tabledir: Union[Path, str] = None,
            commit_interval: Seconds = 9999.0,
            table_cleanup_interval: Seconds = 9999.0,
            key_serializer: CodecArg = 'json',
            value_serializer: CodecArg = 'json',
            num_standby_replicas: int = 0,
            replication_factor: int = 1,
            default_partitions: int = 8,
            reply_to: str = None,
            create_reply_topic: bool = False,
            reply_expires: Seconds = 9999.0,
            Stream: SymbolArg[Type[StreamT]] = '',
            Table: SymbolArg[Type[TableT]] = '',
            TableManager: SymbolArg[Type[TableManagerT]] = '',
            Set: SymbolArg[Type[SetT]] = '',
            Serializers: SymbolArg[Type[RegistryT]] = '',
            Worker: SymbolArg[Type[WorkerT]] = None,
            monitor: Monitor = None,
            on_startup_finished: Callable = None,
            stream_buffer_maxsize: int = 1,
            loop: asyncio.AbstractEventLoop = None) -> None:
        self.on_startup_finished: Callable = None

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
              sink: Iterable[SinkT] = None) -> Callable[[AgentFun], AgentT]:
        ...

    @abc.abstractmethod
    def task(self, fun: TaskArg) -> Callable:
        ...

    @abc.abstractmethod
    def timer(self, interval: Seconds,
              on_leader: bool = False) -> Callable:
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
            loop: asyncio.AbstractEventLoop = None) -> asyncio.Queue:
        ...

    @abc.abstractmethod
    def Worker(self, **kwargs: Any) -> WorkerT:
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
