import abc
from .models import Event
from .services import ServiceT


class SensorT(ServiceT):

    @abc.abstractmethod
    async def on_event_in(
            self, consumer_id: int, offset: int, event: Event) -> None:
        ...

    @abc.abstractmethod
    async def on_event_out(
            self, consumer_id: int, offset: int, event: Event = None) -> None:
        ...
