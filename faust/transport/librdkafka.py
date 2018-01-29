"""Message transport using :pypi:`aiokafka`."""
import asyncio
import typing
from enum import Enum
from typing import (
    Any, AsyncIterator, Awaitable, Callable, ClassVar, Dict, Iterable,
    List, Mapping, MutableMapping, NamedTuple, Optional, Set, Tuple, Type,
    cast,
)

import aiokafka
import confluent_kafka
from confluent_kafka import TopicPartition as _TopicPartition
from kafka.errors import (
    NotControllerError, TopicAlreadyExistsError as TopicExistsError, for_code,
)
from kafka.protocol.offset import OffsetResetStrategy
from mode import Seconds, Service, want_seconds
from mode.threads import ServiceThread
from mode.utils.futures import notify

from . import base
from ..types import AppT, Message, RecordMetadata, TP
from ..types.transports import ConsumerT, ProducerT
from ..utils.futures import StampedeWrapper
from ..utils.kafka.protocol.admin import CreateTopicsRequest
from ..utils.objects import cached_property

__all__ = ['Consumer', 'Producer', 'Transport']


class AssignmentType(Enum):
    ASSIGNED = 0x2
    REVOKED = 0x8


class AssignmentRequest(NamedTuple):
    type: AssignmentType
    tps: Set[TP]


class EnqueuedMethod(NamedTuple):
    callback: Callable
    fut: asyncio.Future
    args: Tuple
    kwargs: Dict


class RPCServiceThread(ServiceThread):
    abstract: ClassVar[bool] = True

    if typing.TYPE_CHECKING:
        method_queue: asyncio.Queue[EnqueuedMethod]
    method_queue = None

    def __init__(self,
                 method_queue: asyncio.Queue = None,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.method_queue = method_queue or asyncio.Queue(loop=self.loop)

    def enqueue_method(self,
                       method: Callable,
                       *args: Any, **kwargs: Any) -> asyncio.Future:
        fut = self.loop.create_future()
        self.method_queue.put(EnqueuedMethod(method, fut, args, kwargs))
        return fut

    @Service.task
    async def _method_qeueuer(self) -> None:
        while not self.should_stop:
            method = await self.method_queue.get()
            try:
                result = method.callback(*method.args, **method.kwargs)
            except BaseException as exc:
                method.fut.set_exception(exc)
            else:
                method.fut.set_result(result)


class Consumer(base.Consumer):
    """Kafka consumer using :pypi:`aiokafka`."""

    _consumer: confluent_kafka.Consumer
    fetch_timeout: float = 10.0
    wait_for_shutdown = True

    consumer_stopped_errors: ClassVar[Tuple[Type[BaseException], ...]] = (
        confluent_kafka.KafkaException,
    )

    if typing.TYPE_CHECKING:
        _assignments: asyncio.Queue[AssignmentRequest]
    _assignments = None

    def on_init(self) -> None:
        app = self.transport.app
        transport = cast(Transport, self.transport)
        if app.client_only:
            self._consumer = self._create_client_consumer(app, transport)
        else:
            self._consumer = self._create_worker_consumer(app, transport)
        self._assignments = asyncio.Queue(loop=self.loop)

    async def on_restart(self) -> None:
        self.on_init()

    def _create_worker_consumer(
            self,
            app: AppT,
            transport: 'Transport') -> confluent_kafka.Consumer:
        self._assignor = self._app.assignor
        return confluent_kafka.Consumer({
            'bootstrap.servers': transport.bootstrap_servers,
            'group.id': app.id,
            'client.id': app.client_id,
            'default.topic.config': {
                'auto.offset.reset': 'earliest',
            },
            'enable.auto.commit': False,
            # XXX partition.assignment.strategy is string
            #'partition.assignment.strategy': [self._assignor],
        })

    def _create_client_consumer(
            self,
            app: AppT,
            transport: 'Transport') -> confluent_kafka.Consumer:
        return confluent_kafka.Consumer({
            'bootstrap.servers': transport.bootstrap_servers,
            'client.id': self.app.client_id,
            'enable.auto.commit': True,
            'default.topic.config': {
                'auto.offset.reset': 'earliest',
            },
        })

    async def create_topic(self, topic: str, partitions: int, replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 1000.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        return  # XXX
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

    async def subscribe(self, topics: Iterable[str]) -> None:
        # XXX pattern does not work :/
        self._consumer.subscribe(
            list(topics),
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

    def _on_assign(self,
                   consumer: confluent_kafka.Consumer,
                   partitions: List[_TopicPartition]) -> None:
        self._assignments.put_nowait(AssignmentRequest(
            AssignmentType.ASSIGNED, cast(Set[TP], set(partitions))))

    def _on_revoke(self,
                   consumer: confluent_kafka.Consumer,
                   partitions: List[_TopicPartition]) -> None:
        self._assignments.put_nowait(AssignmentRequest(
            AssignmentType.REVOKED, cast(Set[TP], set(partitions))))

    @Service.task
    async def _assignments_handler(self) -> None:
        while not self.should_stop:
            typ, tps = await self._assignments.get()
            return await {
                AssignmentType.ASSIGNED: self.on_partitions_assigned,
                AssignmentType.REVOKED: self.on_partitions_revoked,
            }[typ](tps)

    async def getmany(
            self,
            *partitions: TP,
            timeout: float) -> AsyncIterator[Tuple[TP, Message]]:
        create_message = Message  # localize
        _sleep = asyncio.sleep
        for _ in range(1000):
            record = self._consumer.poll(timeout=0)
            if record is not None:
                # XXX is the first field really the timestamp type?
                timestamp_type, timestamp = record.timestamp() or (None, None)
                message = create_message(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    timestamp / 1000.0,
                    timestamp_type,
                    record.key(),
                    record.value(),
                    None,  # checksum
                )
                yield message.tp, message
            await _sleep(0, loop=self.loop)

    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        return cast(TP, _TopicPartition(topic, partition))

    async def _commit(self, tp: TP, offset: int, meta: str) -> None:
        self.log.dev('COMMITTING OFFSETS: tp=%r offset=%r', tp, offset)
        self._consumer.commit(offsets=[
            _TopicPartition(tp.topic, tp.partition, offset),
        ])

    async def on_stop(self) -> None:
        await self.commit()
        self._consumer.close()
        cast(Transport, self.transport)._topic_waiters.clear()

    async def _perform_seek(self) -> None:
        return # XXX
        read_offset = self._read_offset
        seek = self._consumer.seek
        for tp in self._consumer.assignment():
            tp = cast(TP, tp)
            checkpoint = await self._consumer.committed(tp)
            if checkpoint is not None:
                read_offset[tp] = checkpoint
                self.log.dev('PERFORM SEEK SOURCE TOPIC: %r -> %r',
                             tp, checkpoint)
                seek(tp, checkpoint)
            else:
                self.log.dev('PERFORM SEEK AT BEGINNING TOPIC: %r', tp)
                await self._seek_to_beginning(tp)

    async def pause_topics(self, topics: Iterable[str]) -> None:
        return # XXX
        for tp in self.assignment():
            if tp.topic in topics:
                self._consumer._subscription.pause(partition=tp)

    async def pause_partitions(self, tps: Iterable[TP]) -> None:
        return # XXX
        for tp in tps:
            self._consumer._subscription.pause(partition=tp)

    async def resume_topics(self, topics: Iterable[str]) -> None:
        return # XXX
        for tp in self.assignment():
            if tp.topic in topics:
                self._consumer._subscription.resume(partition=tp)

    async def resume_partitions(self, tps: Iterable[TP]) -> None:
        return # XXX
        for tp in tps:
            self._consumer._subscription.resume(partition=tp)

    async def position(self, tp: TP) -> Optional[int]:
        return await self._consumer.position(tp)

    async def _seek_to_beginning(self, *partitions: TP) -> None:
        return # XXX
        for partition in partitions:
            self.log.dev('SEEK TO BEGINNING: %r', partition)
            self._consumer._subscription.need_offset_reset(
                partition, OffsetResetStrategy.EARLIEST)

    async def seek(self, partition: TP, offset: int) -> None:
        return # XXX
        self.log.dev('SEEK %r -> %r', partition, offset)
        self._read_offset[partition] = offset
        self._consumer.seek(partition, offset)

    def assignment(self) -> Set[TP]:
        return cast(Set[TP], self._consumer.assignment())

    def highwater(self, tp: TP) -> int:
        return self._consumer.highwater(tp)

    async def earliest_offsets(self,
                               *partitions: TP) -> MutableMapping[TP, int]:
        return {tp: -1 for tp in partitions} # XXX
        return await self._consumer.beginning_offsets(partitions)

    async def highwaters(self, *partitions: TP) -> MutableMapping[TP, int]:
        return {tp: -1 for tp in partitions}  # XXX
        return await self._consumer.end_offsets(partitions)


class ProducerThread(RPCServiceThread):
    producer: 'Producer' = None
    transport: 'Transport' = None
    if typing.TYPE_CHECKING:
        method_queue: asyncio.Queue[EnqueuedMethod]
    method_queue = None

    _producer: confluent_kafka.Producer = None
    _flush_soon: asyncio.Future = None

    def __init__(self, producer: ProducerT, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.producer = producer
        self.transport = cast(Transport, producer.transport)

    async def on_start(self) -> None:
        self._producer = confluent_kafka.Producer({
            'bootstrap.servers': self.transport.bootstrap_servers,
            'client.id': self.transport.app.client_id,
            'max.in.flight.requests.per.connection': 1,
        })

    async def on_stop(self) -> None:
        if self._producer is not None:
            self._producer.flush()

    def produce(self,
                topic: str,
                key: bytes,
                value: bytes,
                partition: int,
                on_delivery: Callable) -> None:
        if partition is not None:
            self._producer.produce(
                topic, key, value, partition, on_delivery=on_delivery)
        else:
            self._producer.produce(
                topic, key, value, on_delivery=on_delivery)
        notify(self._flush_soon)

    @Service.task
    async def _background_flush(self) -> None:
        producer = self._producer
        _flush = producer.flush
        _sleep = self.sleep
        _create_future = self.loop.create_future
        while not self.should_stop:
            flush_soon = self._flush_soon
            if flush_soon is None:
                flush_soon = self._flush_soon = _create_future()
            try:
                await flush_soon
            finally:
                self._flush_soon = None

            await _sleep(0)
            _flush(timeout=1)


class ProducerProduceFuture(asyncio.Future):

    def set_from_on_delivery(self,
                             err: Optional[BaseException],
                             msg: confluent_kafka.Message) -> None:
        if err:
            # XXX Not sure what err' is here, hopefully it's an exception
            # object and not a string [ask].
            self.set_exception(err)
        else:
            self.set_result(self.message_to_metadata(msg))

    def message_to_metadata(
            self, message: confluent_kafka.Message) -> RecordMetadata:
        topic, partition = tp = TP(message.topic(), message.partition())
        return RecordMetadata(topic, partition, tp, message.offset())


class Producer(base.Producer):
    """Kafka producer using :pypi:`confluent_kafka`."""

    _producer_thread: ProducerThread
    _quick_produce: Any = None

    def on_init(self) -> None:
        self._producer_thread = ProducerThread(
            self, loop=self.loop, beacon=self.beacon)
        self._quick_produce = self._producer_thread.produce

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
        return
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
        await self._producer_thread.start()

    async def on_stop(self) -> None:
        cast(Transport, self.transport)._topic_waiters.clear()
        await self._producer_thread.stop()

    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes],
            partition: Optional[int]) -> Awaitable[RecordMetadata]:
        fut = ProducerProduceFuture(loop=self.loop)
        self._quick_produce(
            topic, value, key, partition, on_delivery=fut.set_from_on_delivery)
        return cast(Awaitable[RecordMetadata], fut)

    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes],
            partition: Optional[int]) -> RecordMetadata:
        return await (await self.send(topic, key, value, partition))

    def key_partition(self, topic: str, key: bytes) -> TP:
        raise NotImplementedError()


class Transport(base.Transport):
    """Kafka transport using :pypi:`aiokafka`."""

    Consumer: ClassVar[Type[ConsumerT]] = Consumer
    Producer: ClassVar[Type[ProducerT]] = Producer

    default_port = 9092
    _version = '-'.join(map(str, confluent_kafka.version()))
    _libversion = '-'.join(map(str, confluent_kafka.libversion()))
    driver_version = f'confluent_kafka={_version} librdkafka={_libversion}'

    _topic_waiters: MutableMapping[str, StampedeWrapper]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._topic_waiters = {}

    @cached_property
    def bootstrap_servers(self) -> str:
        # remove the scheme
        servers = str(self.url).split('://', 1)[1]
        # add default ports
        return ';'.join(
            host if ':' in host else f'{host}:{self.default_port}'
            for host in servers.split(';')
        )

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
        extra_configs = config or {}
        config = self._topic_config(retention, compacting, deleting)
        config.update(extra_configs)

        # Create topic request needs to be sent to the kafka cluster controller
        # Since aiokafka client doesn't currently support MetadataRequest
        # version 1, client.controller will always be None. Hence we cycle
        # through all brokers if we get Error 41 (not controller) until we
        # hit the controller
        nodes = [broker.nodeId for broker in client.cluster.brokers()]
        owner.log.info(f'Nodes: {nodes}')
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
        raise Exception(f'No controller found among brokers: {nodes}')
