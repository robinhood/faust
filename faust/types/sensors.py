import abc
from faust.utils.types.services import ServiceT
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
    async def on_message_out(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message = None) -> None:
        ...
