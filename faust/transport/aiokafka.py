import aiokafka
from typing import Awaitable, Optional, cast
from ..types import Message
from ..utils.objects import cached_property
from . import base


class Consumer(base.Consumer):
    _consumer: aiokafka.AIOKafkaConsumer

    def on_init(self) -> None:
        transport = cast(Transport, self.transport)
        self._consumer = aiokafka.AIOKafkaConsumer(
            *self.topic.topics or (),
            loop=self.loop,
            client_id=self.client_id,
            group_id='hello',
            bootstrap_servers=transport.bootstrap_servers,
        )

    async def on_start(self) -> None:
        await self._consumer.start()
        await self.register_timers()
        self.add_poller(self.drain_events)

    async def on_stop(self) -> None:
        await self._consumer.stop()

    async def drain_events(self, *, timeout: float = 10.0) -> None:
        records = await self._consumer.getmany(
            max_records=10, timeout_ms=timeout * 1000.0)
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
        return self._producer.send(topic, value, key=key)

    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        return self._producer.send_and_wait(topic, value, key=key)


class Transport(base.Transport):
    Consumer: type = Consumer
    Producer: type = Producer

    @cached_property
    def bootstrap_servers(self):
        return self.url.split('://', 1)[1]
