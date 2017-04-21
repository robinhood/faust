from .types import Message, SensorT, TopicPartition
from .utils.services import Service


class Sensor(SensorT, Service):

    async def on_message_in(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message) -> None:
        # WARNING: Sensors must never keep a reference to the Message,
        #          as this means the message won't go out of scope!
        print('SENSOR: ON MSG IN %r %r %r' % (consumer_id, offset, message))

    async def on_message_out(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message = None) -> None:
        print('SENSOR: ON MSG OUT: %r %r %r' % (consumer_id, offset, message))
