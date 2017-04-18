import abc
from faust.utils.types.services import ServiceT
from .tuples import Message


class SensorT(ServiceT):

    @abc.abstractmethod
    async def on_message_in(
            self,
            consumer_id: int,
            offset: int,
            message: Message) -> None:
        ...

    @abc.abstractmethod
    async def on_message_out(
            self,
            consumer_id: int,
            offset: int,
            message: Message = None) -> None:
        ...
