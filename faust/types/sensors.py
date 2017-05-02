import abc
from typing import Any, Iterable
from faust.utils.types.services import ServiceT
from .models import Event
from .streams import StreamT, TableT
from .transports import ConsumerT, ProducerT
from .tuples import Message, TopicPartition

__all__ = ['SensorInterfaceT', 'SensorT']


class SensorInterfaceT(abc.ABC):

    @abc.abstractmethod
    async def on_message_in(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message) -> None:
        ...

    @abc.abstractmethod
    async def on_stream_event_in(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: Event) -> None:
        ...

    @abc.abstractmethod
    async def on_stream_event_out(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: Event) -> None:
        ...

    @abc.abstractmethod
    async def on_message_out(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message = None) -> None:
        ...

    @abc.abstractmethod
    def on_table_get(self, table: TableT, key: Any) -> None:
        ...

    @abc.abstractmethod
    def on_table_set(self, table: TableT, key: Any, value: Any) -> None:
        ...

    @abc.abstractmethod
    def on_table_del(self, table: TableT, key: Any) -> None:
        ...

    @abc.abstractmethod
    async def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        ...

    @abc.abstractmethod
    async def on_commit_completed(
            self, consumer: ConsumerT, state: Any) -> None:
        ...

    @abc.abstractmethod
    async def on_send_initiated(
            self, producer: ProducerT, topic: str,
            keysize: int, valsize: int) -> Any:
        ...

    @abc.abstractmethod
    async def on_send_completed(self, producer: ProducerT, state: Any) -> None:
        ...


class SensorT(SensorInterfaceT, ServiceT):
    ...


class SensorDelegateT(SensorInterfaceT, Iterable):

    @abc.abstractmethod
    def add(self, sensor: SensorT) -> None:
        ...

    @abc.abstractmethod
    def remove(self, sensor: SensorT) -> None:
        ...
