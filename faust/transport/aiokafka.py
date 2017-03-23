"""Message transport using :pypi:`aiokafka`."""
import aiokafka
from typing import Awaitable, Optional, Type, cast
from ..types import Message
from ..utils.async import done_future
from ..utils.objects import cached_property
from . import base


class Consumer(base.Consumer):
    _consumer: aiokafka.AIOKafkaConsumer
    fetch_timeout: float = 10.0

    def on_init(self) -> None:
        transport = cast(Transport, self.transport)
        self._consumer = aiokafka.AIOKafkaConsumer(
            *self.topic.topics or (),
            loop=self.loop,
            client_id=transport.app.client_id,
            group_id=transport.app.id,
            bootstrap_servers=transport.bootstrap_servers,
        )

    async def on_start(self) -> None:
        await self._consumer.start()
        await self.register_timers()
        self.add_poller(self.drain_events)

    async def on_stop(self) -> None:
        await self._consumer.stop()

    async def drain_events(self) -> None:
        records = await self._consumer.getmany(
            max_records=10, timeout_ms=self.fetch_timeout * 1000.0)
        on_message = self.on_message
        for messages in records.values():
            for message in messages:
                await on_message(cast(Message, message))

    async def _commit(self, offset: int) -> None:
        await self._consumer.commit(offset)


class Producer(base.Producer):
    _producer: aiokafka.AIOKafkaProducer

    def on_init(self) -> None:
        transport = cast(Transport, self.transport)
        self._producer = aiokafka.AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=transport.bootstrap_servers,
            client_id=transport.app.client_id,
        )

    async def on_start(self) -> None:
        await self._producer.start()

    async def on_stop(self) -> None:
        await self._producer.stop()

    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        await self._producer.send(topic, value, key=key)
        return done_future(loop=self.loop)  # interface excepts Awaitable

    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        return await self._producer.send_and_wait(topic, value, key=key)


class Transport(base.Transport):
    Consumer: Type = Consumer
    Producer: Type = Producer

    @cached_property
    def bootstrap_servers(self):
        return self.url.split('://', 1)[1]
