import abc
import typing
from typing import Any, Iterable

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

__all__ = ['SensorInterfaceT', 'SensorT', 'SensorDelegateT']


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
