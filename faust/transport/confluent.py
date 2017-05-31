"""Message transport using :pypi:`aiokafka`."""
import confluent_kafka
from typing import Awaitable, Optional, cast
from ..types import Message
from ..utils.futures import done_future
from ..utils.objects import cached_property
from ..utils.services import Service
from . import base

__all__ = ['Consumer', 'Producer', 'Transport']


class Consumer(base.Consumer):
    _consumer: confluent_kafka.Consumer
    fetch_timeout: float = 10.0
    wait_for_shutdown = False

    def on_init(self) -> None:
        transport = cast(Transport, self.transport)
        self._consumer = confluent_kafka.Consumer({
            'bootstrap.servers': transport.bootstrap_servers,
            # 'client.id': transport.app.client_id,
            'group.id': transport.app.id,
            'default.topic.config': {'auto.offset.reset': 'smallest'},
        })

    async def on_start(self) -> None:
        self.beacon.add(self._consumer)
        self._consumer.subscribe(list(self.topic.topics))

    @Service.task
    async def _drain_messages(self) -> None:
        on_message = self.on_message
        poll = self._consumer.poll
        should_stop = self._stopped.is_set
        try:
            while not should_stop():
                message = poll()
                _key = message.key()
                _value = message.value()
                await on_message(Message(
                    key=_key or None,
                    value=_value or None,
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    timestamp=message.timestamp(),
                    timestamp_type='timestamp',
                    checksum='',
                    serialized_key_size=len(_key) if _key else 0,
                    serialized_value_size=len(_value) if _value else 0,

                ))
        finally:
            self.set_shutdown()

    async def _commit(self, offset: int) -> None:
        await self._consumer.commit(offset)


class Producer(base.Producer):
    _producer: confluent_kafka.Producer

    def on_init(self) -> None:
        transport = cast(Transport, self.transport)
        self._producer = confluent_kafka.Producer({
            'bootstrap.servers': transport.bootstrap_servers,
            #  'client.id': transport.app.client_id,
        })

    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        self._producer.produce(topic, value, key=key)
        return done_future(loop=self.loop)  # interface expects Awaitable

    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        self._producer.produce(topic, value, key=key)
        self._producer.flush()
        return done_future(loop=self.loop)  # interface expects Awaitable


class Transport(base.Transport):
    Consumer = Consumer
    Producer = Producer

    @cached_property
    def bootstrap_servers(self):
        return self.url.split('://', 1)[1]  # just remove the scheme
