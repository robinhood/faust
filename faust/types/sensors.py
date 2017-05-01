import abc
from typing import Any
from faust.utils.types.services import ServiceT
from .models import Event
from .streams import StreamT, TableT
from .tuples import Message, TopicPartition


class SensorT(ServiceT):

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
