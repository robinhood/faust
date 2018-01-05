"""Base-interface for sensors."""
from typing import Any, Iterator, Set
from mode import Service
from ..types import AppT, CollectionT, EventT, Message, StreamT, TP
from ..types.sensors import SensorDelegateT, SensorT
from ..types.transports import ConsumerT, ProducerT

__all__ = ['Sensor', 'SensorDelegate']


class Sensor(SensorT, Service):
    """Base class for sensors.

    This sensor does not do anything at all, but can be subclassed
    to create new monitors.
    """

    async def on_message_in(
            self,
            consumer_id: int,
            tp: TP,
            offset: int,
            message: Message) -> None:
        """Message received by a consumer."""
        # WARNING: Sensors must never keep a reference to the Message,
        #          as this means the message won't go out of scope!
        ...

    async def on_stream_event_in(
            self,
            tp: TP,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        """Message sent to a stream as an event."""
        ...

    async def on_stream_event_out(
            self,
            tp: TP,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        """Event was acknowledged by stream.

        Notes:
            Acknowledged means a stream finished processing the event, but
            given that multiple streams may be handling the same event,
            the message can not be committed before all streams have
            processed it.  When all streams have acknowledged the event,
            it will go through :meth:`on_message_out` just before offsets
            are committed.
        """
        ...

    async def on_message_out(
            self,
            consumer_id: int,
            tp: TP,
            offset: int,
            message: Message = None) -> None:
        """All streams finished processing message."""
        ...

    def on_table_get(self, table: CollectionT, key: Any) -> None:
        """Key retrieved from table."""
        ...

    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        """Value set for key in table."""
        ...

    def on_table_del(self, table: CollectionT, key: Any) -> None:
        """Key deleted from table."""
        ...

    async def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        """Consumer is about to commit topic offset."""
        ...

    async def on_commit_completed(
            self, consumer: ConsumerT, state: Any) -> None:
        """Consumer finished committing topic offset."""
        ...

    async def on_send_initiated(
            self, producer: ProducerT, topic: str,
            keysize: int, valsize: int) -> Any:
        """About to send a message."""
        ...

    async def on_send_completed(
            self, producer: ProducerT, state: Any) -> None:
        """Message successfully sent."""


class SensorDelegate(SensorDelegateT):
    """A class that delegates sensor methods to a list of sensors."""

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
            tp: TP,
            offset: int,
            message: Message) -> None:
        for sensor in self._sensors:
            await sensor.on_message_in(consumer_id, tp, offset, message)

    async def on_stream_event_in(
            self,
            tp: TP,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        for sensor in self._sensors:
            await sensor.on_stream_event_in(tp, offset, stream, event)

    async def on_stream_event_out(
            self,
            tp: TP,
            offset: int,
            stream: StreamT,
            event: EventT) -> None:
        for sensor in self._sensors:
            await sensor.on_stream_event_out(tp, offset, stream, event)

    async def on_message_out(
            self,
            consumer_id: int,
            tp: TP,
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
