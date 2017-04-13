"""Message transport using :pypi:`aiokafka`."""
import aiokafka
import asyncio
from kafka.consumer import subscription_state
from kafka.structs import TopicPartition as _TopicPartition
from typing import Awaitable, ClassVar, Optional, Sequence, Type, cast
from ..types import Message, TopicPartition
from ..types.transports import ConsumerT
from ..utils.futures import done_future
from ..utils.objects import cached_property
from . import base

__all__ = ['Consumer', 'Producer', 'Transport']


class ConsumerRebalanceListener(subscription_state.ConsumerRebalanceListener):
    # kafka's ridiculous class based callback interface makes this hacky.

    def __init__(self, consumer: ConsumerT) -> None:
        self.consumer: ConsumerT = consumer

    def on_partitions_assigned(self,
                               assigned: Sequence[_TopicPartition]) -> None:
        # have to cast to Consumer since ConsumerT interface does not
        # have this attribute (mypy currently thinks a Callable instance
        # variable is an instance method).  Furthermore we have to cast
        # the Kafka TopicPartition namedtuples to our description,
        # that way they are typed and decoupled from the actual client
        # implementation.
        return cast(Consumer, self.consumer)._on_partitions_assigned(
            cast(Sequence[TopicPartition], assigned))

    def on_partitions_revoked(self,
                              revoked: Sequence[_TopicPartition]) -> None:
        # see comment in on_partitions_assigned
        return cast(Consumer, self.consumer)._on_partitions_revoked(
            cast(Sequence[TopicPartition], revoked))


class Consumer(base.Consumer):
    RebalanceListener: ClassVar[Type] = ConsumerRebalanceListener
    _consumer: aiokafka.AIOKafkaConsumer
    fetch_timeout: float = 10.0
    wait_for_shutdown = False

    def on_init(self) -> None:
        transport = cast(Transport, self.transport)
        self._consumer = aiokafka.AIOKafkaConsumer(
            loop=self.loop,
            client_id=transport.app.client_id,
            group_id=transport.app.id,
            bootstrap_servers=transport.bootstrap_servers,
        )

    async def on_start(self) -> None:
        self.beacon.add(self._consumer)
        await self._consumer.start()
        await self.register_timers()
        asyncio.ensure_future(self._drain_messages(), loop=self.loop)

    async def subscribe(self, pattern: str) -> None:
        # XXX pattern does not work :/
        self._consumer.subscribe(
            topics=pattern.split('|'),
            #listener=self._rebalance_listener,
        )

    async def on_stop(self) -> None:
        await self._consumer.stop()

    async def _drain_messages(self) -> None:
        callback = self.callback
        getone = self._consumer._fetcher.next_record
        track_message = self.track_message
        should_stop = self._stopped.is_set
        try:
            while not should_stop():
                message = Message.from_message(await getone(()))
                track_message(message, message.offset)
                await callback(message)
        finally:
            self.set_shutdown()

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
        self.beacon.add(self._producer)
        await self._producer.start()

    async def on_stop(self) -> None:
        await self._producer.stop()

    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes]) -> Awaitable:
        await self._producer.send(topic, value, key=key)
        return done_future(loop=self.loop)  # interface expects Awaitable

    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes]) -> Awaitable:
        return await self._producer.send_and_wait(topic, value, key=key)


class Transport(base.Transport):
    Consumer: ClassVar[Type] = Consumer
    Producer: ClassVar[Type] = Producer

    default_port = 9092

    @cached_property
    def bootstrap_servers(self) -> str:
        # remove the scheme
        servers = self.url.split('://', 1)[1]
        # add default ports
        return ';'.join(
            (host if ':' in host else '{}:{}'.format(host, self.default_port))
            for host in servers.split(';')
        )
