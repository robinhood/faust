import abc
from .tuples import Message
from .services import ServiceT


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
