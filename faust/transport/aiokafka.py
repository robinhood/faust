"""Message transport using :pypi:`aiokafka`."""
import asyncio
from typing import Any, Awaitable, ClassVar, Optional, Sequence, Type, cast

import aiokafka
from aiokafka.errors import ConsumerStoppedError
from kafka.consumer import subscription_state
from kafka.structs import (
    OffsetAndMetadata,
    TopicPartition as _TopicPartition,
)

from faust.assignor.partition_assignor import PartitionAssignor
from . import base
from ..types import Message, TopicPartition
from ..types.transports import ConsumerT
from ..utils.futures import done_future
from ..utils.logging import get_logger
from ..utils.objects import cached_property

__all__ = ['Consumer', 'Producer', 'Transport']

logger = get_logger(__name__)


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
    wait_for_shutdown = True
    _assignor = PartitionAssignor()

    def on_init(self) -> None:
        transport = cast(Transport, self.transport)
        self._consumer = aiokafka.AIOKafkaConsumer(
            loop=self.loop,
            client_id=transport.app.client_id,
            group_id=transport.app.id,
            bootstrap_servers=transport.bootstrap_servers,
            partition_assignment_strategy=[self._assignor],
        )

    async def on_start(self) -> None:
        self.beacon.add(self._consumer)
        await self._consumer.start()
        await self.register_timers()
        self.add_future(self._drain_messages())

    async def subscribe(self, pattern: str) -> None:
        # XXX pattern does not work :/
        self._consumer.subscribe(
            topics=pattern.split('|'),
            #listener=self._rebalance_listener,
        )

    def _get_topic_meta(self, topic: str) -> Any:
        return self._consumer.partitions_for_topic(topic)

    def _new_topicpartition(
            self, topic: str, partition: int) -> TopicPartition:
        return cast(TopicPartition, _TopicPartition(topic, partition))

    def _new_offsetandmetadata(self, offset: int, meta: Any) -> Any:
        return OffsetAndMetadata(offset, meta)

    async def on_stop(self) -> None:
        await self.commit()
        await self._consumer.stop()

    async def _drain_messages(self) -> None:
        callback = self.callback
        getmany = self._consumer.getmany
        track_message = self.track_message
        should_stop = self._stopped.is_set
        gather = asyncio.gather
        loop = self.loop

        async def deliver(record: Any, tp: TopicPartition) -> None:
            message = Message.from_message(record, tp)
            await track_message(message, tp, message.offset)
            await callback(message)

        try:
            while not should_stop():
                pending = []
                records = await getmany(timeout_ms=1000, max_records=None)
                for tp, messages in records.items():
                    pending.extend([
                        deliver(message, tp) for message in messages
                    ])
                await gather(*pending, loop=loop)
        except ConsumerStoppedError:
            if self.transport.app.should_stop:
                # we're already stopping so ignore
                logger.info('Consumer: stopped, shutting down...')
                return
            raise
        except Exception as exc:
            logger.exception('Drain messages raised: %r', exc)
        finally:
            self.set_shutdown()

    async def _commit(self, offsets: Any) -> None:
        await self._consumer.commit(offsets)


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
            value: Optional[bytes],
            partition: Optional[int]) -> Awaitable:
        await self._producer.send(topic, value, key=key)
        return done_future(loop=self.loop)  # interface expects Awaitable

    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes],
            partition: Optional[int]) -> Awaitable:
        return await self._producer.send_and_wait(topic, value, key=key)


class Transport(base.Transport):
    Consumer: ClassVar[Type] = Consumer
    Producer: ClassVar[Type] = Producer

    default_port = 9092
    driver_version = f'aiokafka={aiokafka.__version__}'

    @cached_property
    def bootstrap_servers(self) -> str:
        # remove the scheme
        servers = self.url.split('://', 1)[1]
        # add default ports
        return ';'.join(
            host if ':' in host else f'{host}:{self.default_port}'
            for host in servers.split(';')
        )
