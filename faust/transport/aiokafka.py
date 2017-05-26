"""Message transport using :pypi:`aiokafka`."""
import aiokafka
import asyncio
from typing import (
    Any, Awaitable, ClassVar, Mapping,
    MutableMapping, Optional, Set, Sequence, Type, cast
)

from aiokafka.errors import ConsumerStoppedError
from kafka import errors
from kafka.consumer import subscription_state
from kafka.structs import (
    OffsetAndMetadata,
    TopicPartition as _TopicPartition,
)

from faust.assignor.partition_assignor import PartitionAssignor
from . import base
from .kafka.protocol.admin import CreateTopicsRequest
from ..types import Message, TopicPartition
from ..types.transports import ConsumerT
from ..utils.futures import done_future
from ..utils.logging import get_logger
from ..utils.objects import cached_property
from ..utils.times import Seconds, want_seconds

__all__ = ['Consumer', 'Producer', 'Transport']

logger = get_logger(__name__)

__flake8_MutableMapping_is_used: MutableMapping  # XXX flake8 bug
__flake8_Set_is_used: Set


class TopicExists(errors.BrokerResponseError):
    errno = 36
    message = 'TOPIC_ALREADY_EXISTS'
    description = 'Topic creation was requested, but topic already exists.'
    retriable = False


EXTRA_ERRORS: Mapping[int, Type] = {
    TopicExists.errno: TopicExists
}


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
        return cast(Consumer, self.consumer).on_partitions_assigned(
            cast(Sequence[TopicPartition], assigned))

    def on_partitions_revoked(self,
                              revoked: Sequence[_TopicPartition]) -> None:
        # see comment in on_partitions_assigned
        return cast(Consumer, self.consumer).on_partitions_revoked(
            cast(Sequence[TopicPartition], revoked))


class Consumer(base.Consumer):
    RebalanceListener: ClassVar[Type] = ConsumerRebalanceListener
    _consumer: aiokafka.AIOKafkaConsumer
    fetch_timeout: float = 10.0
    wait_for_shutdown = True
    _assignor: PartitionAssignor

    def on_init(self) -> None:
        transport = cast(Transport, self.transport)
        self._assignor = PartitionAssignor()
        self._consumer = aiokafka.AIOKafkaConsumer(
            loop=self.loop,
            client_id=transport.app.client_id,
            group_id=transport.app.id,
            bootstrap_servers=transport.bootstrap_servers,
            partition_assignment_strategy=[self._assignor],
            enable_auto_commit=False,
        )

    async def create_topic(self, topic: str, partitions: int, replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 1000.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        cast(Transport, self.transport)._create_topic(
            self._consumer._client, topic, partitions, replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=int(want_seconds(retention) * 1000.0),
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    async def on_start(self) -> None:
        self.beacon.add(self._consumer)
        await self._consumer.start()
        await self.register_timers()
        self.add_future(self._drain_messages())

    async def subscribe(self, pattern: str) -> None:
        # XXX pattern does not work :/
        self._consumer.subscribe(
            pattern=pattern,
            listener=self._rebalance_listener,
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
        wait = asyncio.wait
        return_when = asyncio.ALL_COMPLETED
        loop = self.loop
        get_current_offset = self._current_offset.__getitem__

        async def deliver(record: Any, tp: TopicPartition) -> None:
            message = self._to_message(record, tp)
            await track_message(message, tp, message.offset)
            await callback(message)

        try:
            while not should_stop():
                pending = []
                records = await getmany(timeout_ms=1000, max_records=None)
                for tp, messages in records.items():
                    current_offset = get_current_offset(tp)
                    for message in messages:
                        if message.offset > current_offset:
                            # Kafka message timestamp is in milliseconds
                            t_secs = message.timestamp / 1000.0
                            message = message._replace(timestamp=t_secs)
                            pending.append(deliver(message, tp))
                if pending:
                    await wait(pending, loop=loop, return_when=return_when)
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

    @classmethod
    def _to_message(cls, record: Any, tp: TopicPartition) -> Message:
        return Message(
            record.topic,
            record.partition,
            record.offset,
            record.timestamp / 1000.0,
            record.timestamp_type,
            record.key,
            record.value,
            record.checksum,
            record.serialized_key_size,
            record.serialized_value_size,
            tp,
        )

    async def _perform_seek(self) -> None:
        current_offset = self._current_offset
        seek = self._consumer.seek
        for tp in self._consumer.assignment():
            tp = cast(TopicPartition, tp)
            if tp not in current_offset:
                committed = await self._consumer.committed(tp)
                current_offset[tp] = committed
                seek(tp, committed)

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

    async def create_topic(self, topic: str, partitions: int, replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 1000.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        cast(Transport, self.transport)._create_topic(
            self._producer._client, topic, partitions, replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=int(want_seconds(retention) * 1000.0),
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
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

    def _topic_config(self,
                      retention: int = None,
                      compacting: bool = None,
                      deleting: bool = None) -> Mapping[str, Any]:
        config: MutableMapping[str, Any] = {}
        cleanup_flags: Set[str] = set()
        if compacting:
            cleanup_flags |= {'compact'}
        if deleting:
            cleanup_flags |= {'delete'}
        if cleanup_flags:
            config['cleanup.policy'] = ','.join(sorted(cleanup_flags))
        if retention:
            config['retention.ms'] = retention
        return config

    async def _create_topic(self, client: aiokafka.AIOKafkaClient,
                            topic: str,
                            partitions: int,
                            replication: int,
                            *,
                            config: Mapping[str, Any] = None,
                            timeout: int = 10000,
                            retention: int = None,
                            compacting: bool = None,
                            deleting: bool = None,
                            ensure_created: bool = False) -> None:
        logger.info(f'Creating topic {topic}')
        protocol_version = 1
        config = config or self._topic_config(retention, compacting, deleting)
        node_id = next(broker.nodeId for broker in client.cluster.brokers())
        request = CreateTopicsRequest[protocol_version](
            [(topic, partitions, replication, [], list(config.items()))],
            timeout,
            False
        )
        response = await client.send(node_id, request)
        assert len(response.topic_error_codes), 'Single topic requested.'
        _, code, reason = response.topic_error_codes[0]

        if code != 0:
            if not ensure_created and code == TopicExists.errno:
                logger.debug(f'Topic {topic} exists, skipping creation.')
            else:
                raise (EXTRA_ERRORS.get(code) or errors.for_code(code))(
                    f'Cannot create topic: {topic} ({code}): {reason}')
