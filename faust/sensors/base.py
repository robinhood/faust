from typing import Any, Iterator, Set
from ..types import AppT, CollectionT, EventT, Message, StreamT, TopicPartition
from ..types.sensors import SensorDelegateT, SensorT
from ..types.transports import ConsumerT, ProducerT
from ..utils.logging import get_logger
from ..utils.services import Service

__all__ = ['Sensor', 'SensorDelegate']

logger = get_logger(__name__)


class Sensor(SensorT, Service):
    """Base class for sensors.

    This sensor does not do anything at all, but can be subclassed
    to create new monitors.
    """
    logger = logger

    async def on_message_in(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message) -> None:
        """Called whenever a message is received by a consumer."""
        # WARNING: Sensors must never keep a reference to the Message,
        #          as this means the message won't go out of scope!
        ...

    async def on_stream_event_in(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        """Called whenever a message is sent to a stream as an event."""
        ...

    async def on_stream_event_out(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        """Called whenever an event is acknowledged (finished processing)."""
        ...

    async def on_message_out(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message = None) -> None:
        """Called when all streams have processed the message."""
        ...

    def on_table_get(self, table: CollectionT, key: Any) -> None:
        """Called whenever a key is retrieved from a table."""
        ...

    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        """Called whenever a key is updated in a table."""
        ...

    def on_table_del(self, table: CollectionT, key: Any) -> None:
        """Called whenever a key is deleted from a table."""
        ...

    async def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        """Called when a consumer is about to commit the offset."""
        ...

    async def on_commit_completed(
            self, consumer: ConsumerT, state: Any) -> None:
        """Called after the offset is committed."""
        ...

    async def on_send_initiated(
            self, producer: ProducerT, topic: str,
            keysize: int, valsize: int) -> Any:
        """Called when a producer is about to send a message."""
        ...

    async def on_send_completed(
            self, producer: ProducerT, state: Any) -> None:
        """Called after a producer has sent a message."""
        ...


class SensorDelegate(SensorDelegateT):

    _sensors: Set[SensorT]

    def __init__(self, app: AppT) -> None:
        self.app = app
        self._sensors = set()

    def add(self, sensor: SensorT) -> None:
        # connect beacons
        sensor.beacon = self.app.beacon.new(sensor)
        self._sensors.add(sensor)

    def remove(self, sensor: SensorT) -> None:
        self._sensors.remove(sensor)

    def __iter__(self) -> Iterator:
        return iter(self._sensors)

    async def on_message_in(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message) -> None:
        for sensor in self._sensors:
            await sensor.on_message_in(consumer_id, tp, offset, message)

    async def on_stream_event_in(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        for sensor in self._sensors:
            await sensor.on_stream_event_in(tp, offset, stream, event)

    async def on_stream_event_out(
            self,
            tp: TopicPartition,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        for sensor in self._sensors:
            await sensor.on_stream_event_out(tp, offset, stream, event)

    async def on_message_out(
            self,
            consumer_id: int,
            tp: TopicPartition,
            offset: int,
            message: Message = None) -> None:
        for sensor in self._sensors:
            await sensor.on_message_out(consumer_id, tp, offset, message)

    def on_table_get(self, table: CollectionT, key: Any) -> None:
        for sensor in self._sensors:
            sensor.on_table_get(table, key)

    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        for sensor in self._sensors:
            sensor.on_table_set(table, key, value)

    def on_table_del(self, table: CollectionT, key: Any) -> None:
        for sensor in self._sensors:
            sensor.on_table_del(table, key)

    async def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        # This returns arbitrary state, so we return a map from sensor->state.
        return {
            sensor: await sensor.on_commit_initiated(consumer)
            for sensor in self._sensors
        }

    async def on_commit_completed(
            self, consumer: ConsumerT, state: Any) -> None:
        # state is now a mapping from sensor->state, so
        # make sure to correct the correct state to each sensor.
        for sensor in self._sensors:
            await sensor.on_commit_completed(consumer, state[sensor])

    async def on_send_initiated(
            self, producer: ProducerT, topic: str,
            keysize: int, valsize: int) -> Any:
        return {
            sensor: await sensor.on_send_initiated(
                producer, topic, keysize, valsize)
            for sensor in self._sensors
        }

    async def on_send_completed(self, producer: ProducerT, state: Any) -> None:
        for sensor in self._sensors:
            await sensor.on_send_completed(producer, state[sensor])

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self._sensors!r}>'
