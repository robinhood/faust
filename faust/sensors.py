from .types import Event, SensorT
from .utils.services import Service


class Sensor(SensorT, Service):

    async def on_event_in(
            self, consumer_id: int, offset: int, event: Event) -> None:
        # WARNING: Sensors must never keep a reference to the Event,
        #          as this means the event won't go out of scope!
        print('SENSOR: ON EVENT IN %r %r %r' % (consumer_id, offset, event))

    async def on_event_out(
            self, consumer_id: int, offset: int, event: Event = None) -> None:
        print('SENSOR: ON EVENT OUT: %r %r %r' % (consumer_id, offset, event))
