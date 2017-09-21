import abc
import asyncio
import typing

from pathlib import Path
from typing import (
    Any, AsyncIterable, Awaitable, Callable,
    Iterable, List, Mapping, MutableMapping, Pattern, Type, Union,
)

from yarl import URL

from ._coroutines import StreamCoroutine
from .actors import ActorFun, ActorT, SinkT
from .assignor import PartitionAssignorT
from .codecs import CodecArg
from .core import K, V
from .router import RouterT
from .sensors import SensorDelegateT
from .serializers import RegistryT
from .streams import StreamT
from .tables import CheckpointManagerT, SetT, TableManagerT, TableT
from .topics import ChannelT, ConductorT, TopicT
from .transports import ConsumerT, ProducerT, TransportT
from .tuples import MessageSentCallback, RecordMetadata
from .windows import WindowT

from ..utils.futures import FlowControlEvent
from ..utils.imports import SymbolArg
from ..utils.times import Seconds
from ..utils.types.collections import NodeT
from ..utils.types.services import ServiceT

if typing.TYPE_CHECKING:
    from ..bin.base import AppCommand
    from ..sensors.monitor import Monitor
    from .models import ModelArg
    from .web.base import Request, Response, Web
    from .web.views import Site, View
else:
    class AppCommand: ...     # noqa
    class Monitor: ...        # noqa
    class ModelArg: ...       # noqa
    class Request: ...        # noqa
    class Response: ...       # noqa
    class Web: ...            # noqa
    class Site: ...           # noqa
    class View: ...           # noqa

__all__ = ['AppT']


ViewGetHandler = Callable[[Web, Request], Awaitable[Response]]
PageArg = Union[Type[View], ViewGetHandler]


class AppT(ServiceT):
    """Abstract type for the Faust application.

    See Also:
        :class:`faust.App`.
    """

    Stream: Type[StreamT]
    TableType: Type[TableT]
    TableManager: Type[TableManagerT]
    CheckpointManager: Type[CheckpointManagerT]
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
    checkpoint_path: Path
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

    actors: MutableMapping[str, ActorT]
    sensors: SensorDelegateT
    serializers: RegistryT
    pages: List[Site]

    @abc.abstractmethod
    def main(self) -> None:
        ...

    @abc.abstractmethod
    def __init__(self, id: str,
                 *,
                 url: Union[str, URL] = 'aiokafka://localhost:9092',
                 store: Union[str, URL] = 'memory://',
                 avro_registry_url: Union[str, URL] = None,
                 canonical_url: Union[str, URL] = None,
                 client_id: str = '',
                 commit_interval: Seconds = 9999.0,
                 table_cleanup_interval: Seconds = 9999.0,
                 key_serializer: CodecArg = 'json',
                 value_serializer: CodecArg = 'json',
                 num_standby_replicas: int = 0,
                 replication_factor: int = 1,
                 default_partitions: int = 8,
                 reply_to: str = None,
                 reply_expires: Seconds = 9999.0,
                 Stream: SymbolArg[Type[StreamT]] = '',
                 Table: SymbolArg[Type[TableT]] = '',
                 TableManager: SymbolArg[Type[TableManagerT]] = '',
                 CheckpointManager: SymbolArg[Type[CheckpointManagerT]] = '',
                 Set: SymbolArg[Type[SetT]] = '',
                 Serializers: SymbolArg[Type[RegistryT]] = '',
                 monitor: Monitor = None,
                 on_startup_finished: Callable = None,
                 origin: str = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.on_startup_finished: Callable = None

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
              config: Mapping[str, Any] = None) -> TopicT:
        ...

    @abc.abstractmethod
    def channel(self, *,
                key_type: ModelArg = None,
                value_type: ModelArg = None,
                loop: asyncio.AbstractEventLoop = None) -> ChannelT:
        ...

    @abc.abstractmethod
    def actor(self,
              channel: Union[str, ChannelT] = None,
              *,
              name: str = None,
              concurrency: int = 1,
              sink: Iterable[SinkT] = None) -> Callable[[ActorFun], ActorT]:
        ...

    @abc.abstractmethod
    def task(self, fun: Callable[[], Awaitable]) -> Callable:
        ...

    @abc.abstractmethod
    def timer(self, interval: Seconds,
              on_leader: bool = False) -> Callable:
        ...

    @abc.abstractmethod
    def stream(self, channel: AsyncIterable,
               coroutine: StreamCoroutine = None,
               beacon: NodeT = None,
               **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    def Table(self, name: str,
              *,
              default: Callable[[], Any] = None,
              window: WindowT = None,
              partitions: int = None,
              **kwargs: Any) -> TableT:
        ...

    @abc.abstractmethod
    def Set(self, name: str,
            *,
            window: WindowT = None,
            partitions: int = None,
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

    @abc.abstractmethod
    def send_soon(
            self, channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        ...

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

    @property
    @abc.abstractmethod
    def tables(self) -> TableManagerT:
        ...

    @property
    @abc.abstractmethod
    def checkpoints(self) -> CheckpointManagerT:
        ...

    @property
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

    @property
    @abc.abstractmethod
    def flow_control(self) -> FlowControlEvent:
        return FlowControlEvent(loop=self.loop)


__flake8_RecordMetadata_is_used: RecordMetadata  # XXX flake8 bug
