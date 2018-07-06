import abc
import typing
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
    cast,
)

from mode import ServiceT

from .events import EventT
from .streams import StreamT
from .tables import CollectionT
from .topics import TopicT
from .transports import ConsumerT, ProducerT
from .tuples import Message, TP

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa

__all__ = ['SensorInterfaceT', 'SensorT', 'SensorDelegateT', 'SystemCheckT']

T = TypeVar('T')


class SensorInterfaceT(abc.ABC):

    @abc.abstractmethod
    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        ...

    @abc.abstractmethod
    def on_stream_event_in(self, tp: TP, offset: int, stream: StreamT,
                           event: EventT) -> None:
        ...

    @abc.abstractmethod
    def on_stream_event_out(self, tp: TP, offset: int, stream: StreamT,
                            event: EventT) -> None:
        ...

    @abc.abstractmethod
    def on_topic_buffer_full(self, topic: TopicT) -> None:
        ...

    @abc.abstractmethod
    def on_message_out(self,
                       tp: TP,
                       offset: int,
                       message: Message) -> None:
        ...

    @abc.abstractmethod
    def on_table_get(self, table: CollectionT, key: Any) -> None:
        ...

    @abc.abstractmethod
    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        ...

    @abc.abstractmethod
    def on_table_del(self, table: CollectionT, key: Any) -> None:
        ...

    @abc.abstractmethod
    def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        ...

    @abc.abstractmethod
    def on_commit_completed(self, consumer: ConsumerT, state: Any) -> None:
        ...

    @abc.abstractmethod
    def on_send_initiated(self, producer: ProducerT, topic: str,
                          keysize: int, valsize: int) -> Any:
        ...

    @abc.abstractmethod
    def on_send_completed(self, producer: ProducerT, state: Any) -> None:
        ...


class SensorT(SensorInterfaceT, ServiceT):
    ...


class SensorDelegateT(SensorInterfaceT, Iterable):

    # Delegate calls to many sensors.

    @abc.abstractmethod
    def add(self, sensor: SensorT) -> None:
        ...

    @abc.abstractmethod
    def remove(self, sensor: SensorT) -> None:
        ...


class SystemCheckT(Generic[T]):
    name: str
    faults: int
    prev_value: Optional[T]
    state: str

    state_to_severity: ClassVar[Mapping[str, int]] = {}
    faults_to_state: ClassVar[List[Tuple[int, str]]] = []

    def __init__(self,
                 name: str,
                 get_value: Callable[[], T] = None,
                 operator: Callable[[T, T], bool] = None) -> None:
        self._get_value: Callable[[], T] = cast(
            Callable[[], T], get_value)
        self.operator: Callable[[T, T], bool] = cast(
            Callable[[T, T], bool], operator)
        self.default_operator: Callable[[T, T], bool] = cast(
            Callable[[T, T], bool], operator)

    @abc.abstractmethod
    def get_value(self) -> T:
        ...

    @abc.abstractmethod
    def check(self, app: AppT) -> None:
        ...


class SystemChecksT(ServiceT):

    @abc.abstractmethod
    def __init__(self, app: AppT, **kwargs: Any) -> None:
        ...
