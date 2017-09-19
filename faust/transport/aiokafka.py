"""Message transport using :pypi:`aiokafka`."""
from typing import (
    Any, AsyncIterator, Awaitable, ClassVar, Iterable, List,
    Mapping, MutableMapping, Optional, Set, Tuple, Type, cast,
)

import aiokafka
from aiokafka.errors import ConsumerStoppedError
from kafka.consumer import subscription_state
from kafka.errors import (
    NotControllerError, TopicAlreadyExistsError as TopicExistsError, for_code,
)
from kafka.protocol.offset import OffsetResetStrategy
from kafka.structs import (
    OffsetAndMetadata,
    TopicPartition as _TopicPartition,
)

from . import base
from ..types import AppT, Message, RecordMetadata, TopicPartition
from ..types.transports import ConsumerT, ProducerT
from ..utils.futures import StampedeWrapper
from ..utils.kafka.protocol.admin import CreateTopicsRequest
from ..utils.logging import get_logger
from ..utils.objects import cached_property
from ..utils.services import Service
from ..utils.times import Seconds, want_seconds

__all__ = ['Consumer', 'Producer', 'Transport']

logger = get_logger(__name__)


class ConsumerRebalanceListener(subscription_state.ConsumerRebalanceListener):
    # kafka's ridiculous class based callback interface makes this hacky.

    def __init__(self, consumer: ConsumerT) -> None:
        self.consumer: ConsumerT = consumer

    async def on_partitions_assigned(
            self, assigned: Iterable[_TopicPartition]) -> None:
        # have to cast to Consumer since ConsumerT interface does not
        # have this attribute (mypy currently thinks a Callable instance
        # variable is an instance method).  Furthermore we have to cast
        # the Kafka TopicPartition namedtuples to our description,
        # that way they are typed and decoupled from the actual client
        # implementation.
        await cast(Consumer, self.consumer).on_partitions_assigned(
            cast(Iterable[TopicPartition], assigned))

    async def on_partitions_revoked(
            self, revoked: Iterable[_TopicPartition]) -> None:
        # see comment in on_partitions_assigned
        await cast(Consumer, self.consumer).on_partitions_revoked(
            cast(Iterable[TopicPartition], revoked))


class Consumer(base.Consumer):
    logger = logger

    RebalanceListener: ClassVar[Type] = ConsumerRebalanceListener
    _consumer: aiokafka.AIOKafkaConsumer
    fetch_timeout: float = 10.0
    wait_for_shutdown = True

    consumer_stopped_errors: ClassVar[Tuple[Type[Exception], ...]] = (
        ConsumerStoppedError,
    )

    def on_init(self) -> None:
        app = self.transport.app
        transport = cast(Transport, self.transport)
        if app.client_only:
            self._consumer = self._create_client_consumer(app, transport)
        else:
            self._consumer = self._create_worker_consumer(app, transport)

    async def on_restart(self) -> None:
        self.on_init()

    def _create_worker_consumer(
            self,
            app: AppT,
            transport: 'Transport') -> aiokafka.AIOKafkaConsumer:
        self._assignor = self._app.assignor
        return aiokafka.AIOKafkaConsumer(
            loop=self.loop,
            client_id=app.client_id,
            group_id=app.id,
            bootstrap_servers=transport.bootstrap_servers,
            partition_assignment_strategy=[self._assignor],
            enable_auto_commit=False,
        )

    def _create_client_consumer(
            self,
            app: AppT,
            transport: 'Transport') -> aiokafka.AIOKafkaConsumer:
        return aiokafka.AIOKafkaConsumer(
            loop=self.loop,
            client_id=app.client_id,
            bootstrap_servers=transport.bootstrap_servers,
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        )

    async def create_topic(self, topic: str, partitions: int, replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 1000.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        await cast(Transport, self.transport)._create_topic(
            self, self._consumer._client, topic, partitions, replication,
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

    async def subscribe(self, topics: Iterable[str]) -> None:
        # XXX pattern does not work :/
        self._consumer.subscribe(
            topics=topics,
            listener=self._rebalance_listener,
        )

    async def getmany(
            self,
            *partitions: TopicPartition,
            timeout: float) -> AsyncIterator[Tuple[TopicPartition, Message]]:
        records = await self._consumer.getmany(
            *partitions,
            timeout_ms=timeout * 1000.0,
            max_records=None,
        )
        create_message = Message  # localize
        for tp, messages in records.items():
            for message in messages:
                yield tp, create_message(
                    message.topic,
                    message.partition,
                    message.offset,
                    message.timestamp / 1000.0,
                    message.timestamp_type,
                    message.key,
                    message.value,
                    message.checksum,
                    message.serialized_key_size,
                    message.serialized_value_size,
                    tp,
                )

    def _new_topicpartition(
            self, topic: str, partition: int) -> TopicPartition:
        return cast(TopicPartition, _TopicPartition(topic, partition))

    def _new_offsetandmetadata(self, offset: int, meta: str) -> Any:
        return OffsetAndMetadata(offset, meta)

    async def on_stop(self) -> None:
        await self.commit()
        await self._consumer.stop()
        cast(Transport, self.transport)._topic_waiters.clear()

    async def _perform_seek(self) -> None:
        read_offset = self._read_offset
        seek = self._consumer.seek
        for tp in self._consumer.assignment():
            tp = cast(TopicPartition, tp)
            checkpoint = await self._consumer.committed(tp)
            if checkpoint is not None:
                read_offset[tp] = checkpoint
                self.log.dev('PERFORM SEEK SOURCE TOPIC: %r -> %r',
                             tp, checkpoint)
                seek(tp, checkpoint)
            else:
                self.log.dev('PERFORM SEEK AT BEGINNING TOPIC: %r', tp)
                await self.seek_to_beginning(tp)

    async def _commit(self, offsets: Any) -> None:
        self.log.dev('COMMITTING OFFSETS: %r', offsets)
        await self._consumer.commit(offsets)

    async def pause_partitions(self, tps: Iterable[TopicPartition]) -> None:
        for partition in tps:
            self._consumer._subscription.pause(partition=partition)

    async def position(self, tp: TopicPartition) -> Optional[int]:
        return await self._consumer.position(tp)

    async def resume_partitions(self, tps: Iterable[TopicPartition]) -> None:
        for partition in tps:
            self._consumer._subscription.resume(partition=partition)
        # XXX This will actually update our paused partitions
        for partition in tps:
            await self._consumer.position(partition)

    async def seek_to_latest(self, *partitions: TopicPartition) -> None:
        for partition in partitions:
            self.log.dev('SEEK TO LATEST: %r', partition)
            self._consumer._subscription.need_offset_reset(
                partition, OffsetResetStrategy.LATEST)

    async def seek_to_beginning(self, *partitions: TopicPartition) -> None:
        for partition in partitions:
            self.log.dev('SEEK TO BEGINNING: %r', partition)
            self._consumer._subscription.need_offset_reset(
                partition, OffsetResetStrategy.EARLIEST)

    async def seek(self, partition: TopicPartition, offset: int) -> None:
        self.log.dev('SEEK %r -> %r', partition, offset)
        self._consumer.seek(partition, offset)

    def assignment(self) -> Set[TopicPartition]:
        return cast(Set[TopicPartition], self._consumer.assignment())

    def highwater(self, tp: TopicPartition) -> int:
        return self._consumer.highwater(tp)


class Producer(base.Producer):
    logger = logger
    _producer: aiokafka.AIOKafkaProducer

    def on_init(self) -> None:
        transport = cast(Transport, self.transport)
        self._producer = aiokafka.AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=transport.bootstrap_servers,
            client_id=transport.app.client_id,
        )

    async def on_restart(self) -> None:
        self.on_init()

    async def create_topic(self, topic: str, partitions: int, replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 1000.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        _retention = (
            int(want_seconds(retention) * 1000.0)
            if retention else None
        )
        await cast(Transport, self.transport)._create_topic(
            self, self._producer.client, topic, partitions, replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=_retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    async def on_start(self) -> None:
        self.beacon.add(self._producer)
        await self._producer.start()

    async def on_stop(self) -> None:
        cast(Transport, self.transport)._topic_waiters.clear()
        await self._producer.stop()

    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes],
            partition: Optional[int]) -> Awaitable[RecordMetadata]:
        return cast(Awaitable[RecordMetadata], await self._producer.send(
            topic, value, key=key, partition=partition))

    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes],
            partition: Optional[int]) -> RecordMetadata:
        return cast(RecordMetadata, await self._producer.send_and_wait(
            topic, value, key=key, partition=partition))

    def key_partition(self, topic: str, key: bytes) -> TopicPartition:
        return TopicPartition(
            topic=topic,
            partition=self._producer._partition(
                topic,
                partition=None,
                key=None,
                value=None,
                serialized_key=key,
                serialized_value=None,
            ),
        )


class Transport(base.Transport):
    Consumer: ClassVar[Type[ConsumerT]] = Consumer
    Producer: ClassVar[Type[ProducerT]] = Producer

    default_port = 9092
    driver_version = f'aiokafka={aiokafka.__version__}'

    _topic_waiters: MutableMapping[str, StampedeWrapper]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._topic_waiters = {}

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

    async def _create_topic(self,
                            owner: Service,
                            client: aiokafka.AIOKafkaClient,
                            topic: str,
                            partitions: int,
                            replication: int,
                            **kwargs: Any) -> None:
        assert topic is not None
        try:
            wrap = self._topic_waiters[topic]
        except KeyError:
            wrap = self._topic_waiters[topic] = StampedeWrapper(
                self._really_create_topic,
                owner, client, topic, partitions, replication,
                loop=self.loop, **kwargs)
        try:
            await wrap()
        except Exception as exc:
            self._topic_waiters.pop(topic, None)
            raise

    async def _really_create_topic(self,
                                   owner: Service,
                                   client: aiokafka.AIOKafkaClient,
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
        owner.log.info(f'Creating topic {topic}')
        protocol_version = 1
        config = config or self._topic_config(retention, compacting, deleting)

        # Create topic request needs to be sent to the kafka cluster controller
        # Since aiokafka client doesn't currently support MetadataRequest
        # version 1, client.controller will always be None. Hence we cycle
        # through all brokers if we get Error 41 (not controller) until we
        # hit the controller
        nodes = [broker.nodeId for broker in client.cluster.brokers()]
        for node_id in nodes:
            if node_id is None:
                raise RuntimeError('Not connected to Kafka broker')

            request = CreateTopicsRequest[protocol_version](
                [(topic, partitions, replication, [], list(config.items()))],
                timeout,
                False,
            )
            response = await client.send(node_id, request)
            assert len(response.topic_error_codes), 'Single topic requested.'

            _, code, reason = response.topic_error_codes[0]

            if code != 0:
                if not ensure_created and code == TopicExistsError.errno:
                    owner.log.debug(
                        f'Topic {topic} exists, skipping creation.')
                    return
                elif code == NotControllerError.errno:
                    owner.log.debug(f'Broker: {node_id} is not controller.')
                    continue
                else:
                    raise for_code(code)(
                        f'Cannot create topic: {topic} ({code}): {reason}')
            else:
                owner.log.info(f'Topic {topic} created.')
                return
        raise Exception(f'No controller found amount brokers: {nodes}')


__flake8_MutableMapping_is_used: MutableMapping  # XXX flake8 bug
__flake8_Set_is_used: Set
__flake8_List_is_used: List
