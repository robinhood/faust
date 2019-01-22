"""Message transport using :pypi:`aiokafka`."""
import asyncio
from typing import (
    Any,
    Awaitable,
    ClassVar,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
)

import aiokafka
import aiokafka.abc
from aiokafka.errors import (
    CommitFailedError,
    ConsumerStoppedError,
    IllegalStateError,
    KafkaError,
)
from aiokafka.structs import (
    OffsetAndMetadata,
    TopicPartition as _TopicPartition,
)
from rhkafka.errors import (
    NotControllerError,
    TopicAlreadyExistsError as TopicExistsError,
    for_code,
)
from rhkafka.partitioner.default import DefaultPartitioner
from rhkafka.protocol.metadata import MetadataRequest_v1
from mode import Service, get_logger
from mode.utils.futures import StampedeWrapper
from mode.utils.times import Seconds, want_seconds
from yarl import URL

from faust.exceptions import ConsumerNotStarted, ProducerSendError
from faust.transport import base
from faust.transport.consumer import (
    ConsumerThread,
    RecordMap,
    ThreadDelegateConsumer,
    ensure_TPset,
)
from faust.types import ConsumerMessage, RecordMetadata, TP
from faust.types.transports import (
    ConsumerT,
    PartitionerT,
    ProducerT,
    TransactionProducerT,
)
from faust.utils.kafka.protocol.admin import CreateTopicsRequest

__all__ = ['Consumer', 'Producer', 'Transport']

if not hasattr(aiokafka, '__robinhood__'):
    raise RuntimeError(
        'Please install robinhood-aiokafka, not aiokafka')

logger = get_logger(__name__)


def server_list(urls: List[URL], default_port: int) -> List[str]:
    default_host = '127.0.0.1'
    return [f'{u.host or default_host}:{u.port or default_port}' for u in urls]


class ConsumerRebalanceListener(aiokafka.abc.ConsumerRebalanceListener):
    # kafka's ridiculous class based callback interface makes this hacky.

    def __init__(self, thread: ConsumerThread) -> None:
        self._thread: ConsumerThread = thread

    async def on_partitions_revoked(
            self, revoked: Iterable[_TopicPartition]) -> None:
        await self._thread.on_partitions_revoked(ensure_TPset(revoked))

    async def on_partitions_assigned(
            self, assigned: Iterable[_TopicPartition]) -> None:
        await self._thread.on_partitions_assigned(ensure_TPset(assigned))


class Consumer(ThreadDelegateConsumer):
    """Kafka consumer using :pypi:`aiokafka`."""
    logger = logger

    RebalanceListener: ClassVar[Type[ConsumerRebalanceListener]]
    RebalanceListener = ConsumerRebalanceListener

    consumer_stopped_errors: ClassVar[Tuple[Type[BaseException], ...]] = (
        ConsumerStoppedError,
    )

    def _new_consumer_thread(self) -> ConsumerThread:
        return AIOKafkaConsumerThread(self, loop=self.loop, beacon=self.beacon)

    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 30.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        await self._thread.create_topic(
            topic,
            partitions,
            replication,
            config=config,
            timeout=timeout,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        return cast(TP, _TopicPartition(topic, partition))

    def _to_message(self, tp: TP, record: Any) -> ConsumerMessage:
        timestamp: Optional[int] = record.timestamp
        timestamp_s: float = cast(float, None)
        if timestamp is not None:
            timestamp_s = timestamp / 1000.0
        return ConsumerMessage(
            record.topic,
            record.partition,
            record.offset,
            timestamp_s,
            record.timestamp_type,
            record.key,
            record.value,
            record.checksum,
            record.serialized_key_size,
            record.serialized_value_size,
            tp,
        )

    async def on_stop(self) -> None:
        await super().on_stop()
        transport = cast(Transport, self.transport)
        transport._topic_waiters.clear()


class AIOKafkaConsumerThread(ConsumerThread):
    _consumer: Optional[aiokafka.AIOKafkaConsumer] = None

    def on_init(self) -> None:
        self._partitioner: PartitionerT = (
            self.app.conf.producer_partitioner or DefaultPartitioner())
        self._rebalance_listener = self.consumer.RebalanceListener(self)

    async def on_start(self) -> None:
        self._consumer = self._create_consumer(loop=self.thread_loop)
        await self._consumer.start()

    async def on_thread_stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()

    def _create_consumer(
            self,
            loop: asyncio.AbstractEventLoop) -> aiokafka.AIOKafkaConsumer:
        transport = cast(Transport, self.transport)
        if self.app.client_only:
            return self._create_client_consumer(transport, loop=loop)
        else:
            return self._create_worker_consumer(transport, loop=loop)

    def _create_worker_consumer(
            self,
            transport: 'Transport',
            loop: asyncio.AbstractEventLoop) -> aiokafka.AIOKafkaConsumer:
        isolation_level: str = 'read_uncommitted'
        conf = self.app.conf
        if self.consumer.in_transaction:
            isolation_level = 'read_committed'
        self._assignor = self.app.assignor
        return aiokafka.AIOKafkaConsumer(
            loop=loop,
            client_id=conf.broker_client_id,
            group_id=conf.id,
            bootstrap_servers=server_list(
                transport.url, transport.default_port),
            partition_assignment_strategy=[self._assignor],
            enable_auto_commit=False,
            auto_offset_reset=conf.consumer_auto_offset_reset,
            max_poll_records=conf.broker_max_poll_records,
            max_partition_fetch_bytes=conf.consumer_max_fetch_size,
            fetch_max_wait_ms=1500,
            request_timeout_ms=int(conf.broker_request_timeout * 1000.0),
            check_crcs=conf.broker_check_crcs,
            session_timeout_ms=int(conf.broker_session_timeout * 1000.0),
            heartbeat_interval_ms=int(conf.broker_heartbeat_interval * 1000.0),
            security_protocol="SSL" if conf.ssl_context else "PLAINTEXT",
            ssl_context=conf.ssl_context,
            isolation_level=isolation_level,
        )

    def _create_client_consumer(
            self,
            transport: 'Transport',
            loop: asyncio.AbstractEventLoop) -> aiokafka.AIOKafkaConsumer:
        conf = self.app.conf
        return aiokafka.AIOKafkaConsumer(
            loop=loop,
            client_id=conf.broker_client_id,
            bootstrap_servers=server_list(
                transport.url, transport.default_port),
            request_timeout_ms=int(conf.broker_request_timeout * 1000.0),
            enable_auto_commit=True,
            max_poll_records=conf.broker_max_poll_records,
            auto_offset_reset=conf.consumer_auto_offset_reset,
            check_crcs=conf.broker_check_crcs,
            security_protocol="SSL" if conf.ssl_context else "PLAINTEXT",
            ssl_context=conf.ssl_context,
        )

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.set_close()
            self._consumer._coordinator.set_close()

    async def subscribe(self, topics: Iterable[str]) -> None:
        # XXX pattern does not work :/
        await self.call_thread(
            self._ensure_consumer().subscribe,
            topics=set(topics),
            listener=self._rebalance_listener,
        )

    async def seek_to_committed(self) -> Mapping[TP, int]:
        return await self.call_thread(
            self._ensure_consumer().seek_to_committed)

    async def commit(self, offsets: Mapping[TP, int]) -> bool:
        return await self.call_thread(self._commit, offsets)

    async def _commit(self, offsets: Mapping[TP, int]) -> bool:
        consumer = self._ensure_consumer()
        try:
            consumer.commit({
                tp: OffsetAndMetadata(offset, '')
                for tp, offset in offsets.items()
            })
        except CommitFailedError as exc:
            if 'already rebalanced' in str(exc):
                return False
            self.log.exception(f'Committing raised exception: %r', exc)
            await self.crash(exc)
            return False
        except IllegalStateError as exc:
            self.log.exception(f'Got exception: {exc}\n'
                               f'Current assignment: {self.assignment()}')
            await self.crash(exc)
            return False
        return True

    async def position(self, tp: TP) -> Optional[int]:
        return await self.call_thread(
            self._ensure_consumer().position, tp)

    async def seek_to_beginning(self, *partitions: _TopicPartition) -> None:
        await self.call_thread(
            self._ensure_consumer().seek_to_beginning, *partitions)

    async def seek_wait(self, partitions: Mapping[TP, int]) -> None:
        consumer = self._ensure_consumer()
        await self.call_thread(self._seek_wait, consumer, partitions)

    async def _seek_wait(self,
                         consumer: Consumer,
                         partitions: Mapping[TP, int]) -> None:
        for tp, offset in partitions.items():
            self.log.dev('SEEK %r -> %r', tp, offset)
            consumer.seek(tp, offset)
        await asyncio.gather(*[
            consumer.position(tp) for tp in partitions
        ])

    def seek(self, partition: TP, offset: int) -> None:
        self._ensure_consumer().seek(partition, offset)

    def assignment(self) -> Set[TP]:
        return ensure_TPset(self._ensure_consumer().assignment())

    def highwater(self, tp: TP) -> int:
        if self.consumer.in_transaction:
            return self._ensure_consumer().last_stable_offset(tp)
        else:
            return self._ensure_consumer().highwater(tp)

    def topic_partitions(self, topic: str) -> Optional[int]:
        if self._consumer is not None:
            return self._consumer._coordinator._metadata_snapshot.get(topic)
        return None

    async def earliest_offsets(self,
                               *partitions: TP) -> Mapping[TP, int]:
        return await self.call_thread(
            self._ensure_consumer().beginning_offsets, partitions)

    async def highwaters(self, *partitions: TP) -> Mapping[TP, int]:

        return await self.call_thread(self._highwaters, partitions)

    async def _highwaters(self, partitions: List[TP]) -> Mapping[TP, int]:
        consumer = self._ensure_consumer()
        if self.consumer.in_transaction:
            return {
                tp: self._lso_to_highwater(consumer.last_stable_offset(tp))
                for tp in partitions
            }
        else:
            return cast(Mapping[TP, int],
                        await consumer.end_offsets(partitions))

    def _lso_to_highwater(self, lso: int) -> int:
        return lso - 1 if lso and lso > 0 else lso

    def _ensure_consumer(self) -> aiokafka.AIOKafkaConsumer:
        if self._consumer is None:
            raise ConsumerNotStarted('Consumer thread not yet started')
        return self._consumer

    async def getmany(self,
                      active_partitions: Set[TP],
                      timeout: float) -> RecordMap:
        # Implementation for the Fetcher service.
        _consumer = self._ensure_consumer()
        fetcher = _consumer._fetcher
        if _consumer._closed or fetcher._closed:
            raise ConsumerStoppedError()
        return await self.call_thread(
            fetcher.fetched_records,
            active_partitions,
            timeout=timeout,
        )

    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 30.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        transport = cast(Transport, self.consumer.transport)
        _consumer = self._ensure_consumer()
        _retention = (int(want_seconds(retention) * 1000.0)
                      if retention else None)
        await self.call_thread(
            transport._create_topic,
            self,
            _consumer._client,
            topic,
            partitions,
            replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=_retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    def key_partition(self,
                      topic: str,
                      key: Optional[bytes],
                      partition: int = None) -> int:
        consumer = self._ensure_consumer()
        metadata = consumer._client.cluster
        if partition is not None:
            assert partition >= 0
            assert partition in metadata.partitions_for_topic(topic), \
                'Unrecognized partition'
            return partition

        all_partitions = list(metadata.partitions_for_topic(topic))
        available = list(metadata.available_partitions_for_topic(topic))
        return self._partitioner(key, all_partitions, available)


class Producer(base.Producer):
    """Kafka producer using :pypi:`aiokafka`."""

    logger = logger

    _producer: aiokafka.AIOKafkaProducer

    def on_init(self) -> None:
        self._producer = self._new_producer()

    def _settings_default(self) -> Mapping[str, Any]:
        transport = cast(Transport, self.transport)
        return {
            'bootstrap_servers': server_list(
                transport.url, transport.default_port),
            'client_id': self.client_id,
            'acks': self.acks,
            'linger_ms': self.linger_ms,
            'max_batch_size': self.max_batch_size,
            'max_request_size': self.max_request_size,
            'compression_type': self.compression_type,
            'on_irrecoverable_error': self._on_irrecoverable_error,
            'security_protocol': 'SSL' if self.ssl_context else 'PLAINTEXT',
            'ssl_context': self.ssl_context,
            'partitioner': self.partitioner or DefaultPartitioner(),
            'request_timeout_ms': int(self.request_timeout * 1000),
        }

    def _settings_extra(self) -> Mapping[str, Any]:
        return {}

    def _new_producer(self) -> aiokafka.AIOKafkaProducer:
        return aiokafka.AIOKafkaProducer(
            loop=self.loop,
            **{**self._settings_default(),
               **self._settings_extra()},
        )

    async def _on_irrecoverable_error(self, exc: BaseException) -> None:
        consumer = self.transport.app.consumer
        if consumer is not None:
            await consumer.crash(exc)
        await self.crash(exc)

    async def on_restart(self) -> None:
        self.on_init()

    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 20.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        _retention = (int(want_seconds(retention) * 1000.0)
                      if retention else None)
        await cast(Transport, self.transport)._create_topic(
            self,
            self._producer.client,
            topic,
            partitions,
            replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=_retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    async def on_start(self) -> None:
        self.beacon.add(self._producer)
        self._last_batch = None
        await self._producer.start()

    async def on_stop(self) -> None:
        cast(Transport, self.transport)._topic_waiters.clear()
        self._last_batch = None
        await self._producer.stop()

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int],
                   timestamp: Optional[float]) -> Awaitable[RecordMetadata]:
        try:
            timestamp_ms = timestamp * 1000.0 if timestamp else timestamp
            return cast(Awaitable[RecordMetadata], await self._producer.send(
                topic, value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
            ))
        except KafkaError as exc:
            raise ProducerSendError(f'Error while sending: {exc!r}') from exc

    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int],
                            timestamp: Optional[float]) -> RecordMetadata:
        fut = await self.send(
            topic,
            key=key,
            value=value,
            partition=partition,
            timestamp=timestamp,
        )
        return await fut

    async def flush(self) -> None:
        await self._producer.flush()

    def key_partition(self, topic: str, key: bytes) -> TP:
        partition = self._producer._partition(
            topic,
            partition=None,
            key=None,
            value=None,
            serialized_key=key,
            serialized_value=None,
        )
        return TP(topic, partition)


class TransactionProducer(Producer, base.TransactionProducer):

    async def on_start(self) -> None:
        await super().on_start()
        await self._producer.begin_transaction()

    async def commit(self, offsets: Mapping[TP, int], group_id: str,
                     start_new_transaction: bool = True) -> None:
        await self._producer.send_offsets_to_transaction(offsets, group_id)
        await self._producer.commit_transaction()
        if start_new_transaction:
            await self._producer.begin_transaction()

    async def on_stop(self) -> None:
        if self._producer._txn_manager.is_in_transaction():
            self.log.info(
                'Still in transaction: Aborting current transaction...')
            await self._producer.abort_transaction()

    def _settings_extra(self) -> Mapping[str, Any]:
        return {
            'enable_idempotence': True,
            'acks': 'all',
            'transactional_id': self.transaction_id,
        }


class Transport(base.Transport):
    """Kafka transport using :pypi:`aiokafka`."""

    Consumer: ClassVar[Type[ConsumerT]]
    Consumer = Consumer
    Producer: ClassVar[Type[ProducerT]]
    Producer = Producer
    TransactionProducer: ClassVar[Type[TransactionProducerT]]
    TransactionProducer = TransactionProducer

    default_port = 9092
    driver_version = f'aiokafka={aiokafka.__version__}'

    _topic_waiters: MutableMapping[str, StampedeWrapper]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._topic_waiters = {}

    def _topic_config(self,
                      retention: int = None,
                      compacting: bool = None,
                      deleting: bool = None) -> MutableMapping[str, Any]:
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
                owner,
                client,
                topic,
                partitions,
                replication,
                loop=asyncio.get_event_loop(), **kwargs)
        try:
            await wrap()
        except Exception:
            self._topic_waiters.pop(topic, None)
            raise

    async def _get_controller_node(self,
                                   owner: Service,
                                   client: aiokafka.AIOKafkaClient,
                                   timeout: int = 30000) -> Optional[int]:
        nodes = [broker.nodeId for broker in client.cluster.brokers()]
        for node_id in nodes:
            if node_id is None:
                raise RuntimeError('Not connected to Kafka Broker')
            request = MetadataRequest_v1([])
            wait_result = await owner.wait(
                client.send(node_id, request),
                timeout=timeout,
            )
            if wait_result.stopped:
                owner.log.info(f'Shutting down - skipping creation.')
                return None
            response = wait_result.result
            return response.controller_id
        raise Exception(f'Controller node not found')

    async def _really_create_topic(self,
                                   owner: Service,
                                   client: aiokafka.AIOKafkaClient,
                                   topic: str,
                                   partitions: int,
                                   replication: int,
                                   *,
                                   config: Mapping[str, Any] = None,
                                   timeout: int = 30000,
                                   retention: int = None,
                                   compacting: bool = None,
                                   deleting: bool = None,
                                   ensure_created: bool = False) -> None:
        owner.log.info(f'Creating topic {topic}')

        if topic in client.cluster.topics():
            owner.log.debug(f'Topic {topic} exists, skipping creation.')
            return

        protocol_version = 1
        extra_configs = config or {}
        config = self._topic_config(retention, compacting, deleting)
        config.update(extra_configs)

        controller_node = await self._get_controller_node(owner, client,
                                                          timeout=timeout)
        owner.log.debug(f'Found controller: {controller_node}')

        if controller_node is None:
            if owner.should_stop:
                owner.log.info(f'Shutting down hence controller not found')
                return
            else:
                raise Exception(f'Controller node is None')

        request = CreateTopicsRequest[protocol_version](
            [(topic, partitions, replication, [], list(config.items()))],
            timeout,
            False,
        )
        wait_result = await owner.wait(
            client.send(controller_node, request),
            timeout=timeout,
        )
        if wait_result.stopped:
            owner.log.debug(f'Shutting down - skipping creation.')
            return
        response = wait_result.result

        assert len(response.topic_error_codes), 'single topic'

        _, code, reason = response.topic_error_codes[0]

        if code != 0:
            if not ensure_created and code == TopicExistsError.errno:
                owner.log.debug(
                    f'Topic {topic} exists, skipping creation.')
                return
            elif code == NotControllerError.errno:
                raise RuntimeError(f'Invalid controller: {controller_node}')
            else:
                raise for_code(code)(
                    f'Cannot create topic: {topic} ({code}): {reason}')
        else:
            owner.log.info(f'Topic {topic} created.')
            return
