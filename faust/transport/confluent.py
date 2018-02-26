"""Message transport using :pypi:`confluent-kafka`."""
import asyncio
import typing
from enum import Enum
from typing import (
    Any, AsyncIterator, Awaitable, Callable, ClassVar, Dict, Iterable,
    List, Mapping, MutableMapping, NamedTuple, Optional, Set, Tuple, Type,
    cast,
)

from mode import Seconds, Service, ServiceT, want_seconds
from mode.threads import ServiceThread
from mode.utils.futures import StampedeWrapper, notify
from yarl import URL

from . import base

from ..exceptions import ImproperlyConfigured
from ..types import AppT, Message, RecordMetadata, TP
from ..types.transports import ConsumerT, ProducerT

try:
    import confluent_kafka
    from confluent_kafka import TopicPartition as _TopicPartition
    from confluent_kafka import KafkaException
except ImportError:
    confluent_kafka = None
    class _TopicPartition: ...            # noqa
    class KafkaException(Exception): ...  # noqa

if typing.TYPE_CHECKING:
    from confluent_kafka import Consumer as _Consumer
    from confluent_kafka import Producer as _Producer
    from confluent_kafka import Message as _Message
else:
    class _Consumer: ...  # noqa
    class _Producer: ...  # noqa
    class _Message: ...   # noqa

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
        self.method_queue.put_nowait(EnqueuedMethod(method, fut, args, kwargs))
        return fut

    async def _process_method_requests(self) -> None:
        while not self.should_stop:
            method = await self.method_queue.get()
            try:
                result = await method.callback(*method.args, **method.kwargs)
            except BaseException as exc:
                method.fut.set_exception(exc)
            else:
                method.fut.set_result(result)


def server_list(url: URL, default_port: int) -> str:
    # remove the scheme
    servers = str(url).split('://', 1)[1]
    # add default ports
    return ';'.join(
        host if ':' in host else f'{host}:{default_port}'
        for host in servers.split(';')
    )


class ConsumerThread(RPCServiceThread):
    abstract: ClassVar[bool] = False
    consumer: 'Consumer' = None
    transport: 'Transport' = None
    app: AppT = None
    _consumer: _Consumer = None
    _fetch_requests: asyncio.Queue = None
    wait_for_shutdown = True

    @Service.task
    async def _method_handler(self) -> None:
        await self._process_method_requests()

    def __init__(self, consumer: 'Consumer', **kwargs: Any) -> None:
        self.consumer = consumer
        super().__init__(**kwargs)

    def on_init(self) -> None:
        transport = self.transport = cast(Transport, self.consumer.transport)
        app = self.app = self.transport.app
        if app.client_only:
            self._consumer = self._create_client_consumer(app, transport)
        else:
            self._consumer = self._create_worker_consumer(app, transport)
        self.beacon.add(self._consumer)
        self._fetch_requests = asyncio.Queue(loop=self.loop)

    async def on_restart(self) -> None:
        self.on_init()

    async def on_thread_stop(self) -> None:
        await self.consumer._commit_all()
        self._consumer.close()
        self.transport._topic_waiters.clear()

    def _create_worker_consumer(
            self,
            app: AppT,
            transport: 'Transport') -> _Consumer:
        self._assignor = self.app.assignor
        return confluent_kafka.Consumer({
            'bootstrap.servers': server_list(
                transport.url, transport.default_port),
            'group.id': app.conf.id,
            'client.id': app.conf.broker_client_id,
            'default.topic.config': {
                'auto.offset.reset': 'earliest',
            },
            'enable.auto.commit': False,
            # XXX partition.assignment.strategy is string
            # need to write C wrapper for this.
            #  'partition.assignment.strategy': [self._assignor],
        })

    def _create_client_consumer(
            self,
            app: AppT,
            transport: 'Transport') -> _Consumer:
        return confluent_kafka.Consumer({
            'bootstrap.servers': server_list(
                transport.url, transport.default_port),
            'client.id': self.app.conf.broker_client_id,
            'enable.auto.commit': True,
            'default.topic.config': {
                'auto.offset.reset': 'earliest',
            },
        })

    async def subscribe(self, topics: Iterable[str]) -> None:
        # XXX pattern does not work :/
        self._consumer.subscribe(
            list(topics),
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

    def _on_assign(self,
                   consumer: _Consumer,
                   partitions: List[_TopicPartition]) -> None:
        self.consumer._assignments.put_nowait(AssignmentRequest(
            AssignmentType.ASSIGNED, cast(Set[TP], set(partitions))))

    def _on_revoke(self,
                   consumer: _Consumer,
                   partitions: List[_TopicPartition]) -> None:
        self.consumer._assignments.put_nowait(AssignmentRequest(
            AssignmentType.REVOKED, cast(Set[TP], set(partitions))))

    async def _commit(self, tp: TP, offset: int, meta: str) -> None:
        self.log.dev('COMMITTING OFFSETS: tp=%r offset=%r', tp, offset)
        self._consumer.commit(offsets=[
            _TopicPartition(tp.topic, tp.partition, offset),
        ])

    async def _drain_messages(self, fetcher: ServiceT) -> None:
        self._fetch_requests.put_nowait(fetcher)

    @Service.task
    async def _drainer(self) -> None:
        while not self.should_stop:
            fetcher: ServiceT = await self._fetch_requests.get()
            await self.consumer._really_drain(fetcher)


class Consumer(base.Consumer):
    """Kafka consumer using :pypi:`confluent-kafka`."""

    _consumer_thread: ConsumerThread = None
    fetch_timeout: float = 10.0
    wait_for_shutdown = True

    consumer_stopped_errors: ClassVar[Tuple[Type[BaseException], ...]] = (
        KafkaException,
    )

    if typing.TYPE_CHECKING:
        _assignments: asyncio.Queue[AssignmentRequest]
    _assignments = None

    def on_init(self) -> None:
        self._assignments = asyncio.Queue(loop=self.loop)

    async def on_start(self) -> None:
        self._consumer_thread = ConsumerThread(
            self, loop=self.loop, beacon=self.beacon)
        await self._consumer_thread.start()

    async def on_stop(self) -> None:
        if self._consumer_thread is not None:
            await self._consumer_thread.stop()
            self._consumer_thread = None

    async def on_restart(self) -> None:
        self.on_init()

    async def _drain_messages(self, fetcher: ServiceT) -> None:
        await self._consumer_thread._drain_messages(fetcher)

    async def _really_drain(self, fetcher: ServiceT) -> None:
        await super()._drain_messages(fetcher)

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

    async def subscribe(self, topics: Iterable[str]) -> None:
        await self._consumer_thread.enqueue_method(
            self._consumer_thread.subscribe, list(topics))

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
        # NOTE: This must execute in the ConsumerThread loop.
        create_message = Message  # localize
        _sleep = asyncio.sleep
        poll = self._consumer_thread._consumer.poll
        buf: List[Message] = []
        loop_time = self.loop.time
        timeout = timeout or 1.0
        end_time = loop_time() + timeout
        # can maybe reduce to 1k here, but 100 was too small and degrades
        # performance.
        max_messages = 10000

        while len(buf) < max_messages:
            time_left = max(end_time - loop_time(), 0)
            if not time_left:
                break
            record = poll(timeout=time_left)
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
                buf.append(message)
            await _sleep(0, loop=self.loop)
        for message in buf:
            yield message.tp, message

    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        return cast(TP, _TopicPartition(topic, partition))

    async def _commit(self, tp: TP, offset: int, meta: str) -> None:
        await self._consumer_thread.enqueue_method(
            self._consumer_thread._commit, tp, offset, meta)

    async def _commit_all(self) -> None:
        await super().commit()

    async def _perform_seek(self) -> None:
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

    async def pause_partitions(self, tps: Iterable[TP]) -> None:
        ...  # XXX needs implementation

    async def resume_partitions(self, tps: Iterable[TP]) -> None:
        ...  # XXX needs implementation

    async def position(self, tp: TP) -> Optional[int]:
        return await self._consumer.position(tp)

    async def _seek_to_beginning(self, *partitions: TP) -> None:
        ...  # XXX needs implementation

    async def seek(self, partition: TP, offset: int) -> None:
        ...  # XXX needs implementation

    def assignment(self) -> Set[TP]:
        return cast(Set[TP], self._consumer_thread._consumer.assignment())

    def highwater(self, tp: TP) -> int:
        return self._consumer_thread._consumer.highwater(tp)

    async def earliest_offsets(self,
                               *partitions: TP) -> MutableMapping[TP, int]:
        return {tp: -1 for tp in partitions}  # XXX needs implementation
        return await self._consumer.beginning_offsets(partitions)

    async def highwaters(self, *partitions: TP) -> MutableMapping[TP, int]:
        return {tp: -1 for tp in partitions}  # XXX needs implementation
        return await self._consumer.end_offsets(partitions)


class ProducerThread(RPCServiceThread):
    producer: 'Producer' = None
    transport: 'Transport' = None
    if typing.TYPE_CHECKING:
        method_queue: asyncio.Queue[EnqueuedMethod]
    method_queue = None

    _producer: _Producer = None
    _flush_soon: asyncio.Future = None

    def __init__(self, producer: ProducerT, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.producer = producer
        self.transport = cast(Transport, producer.transport)

    async def on_start(self) -> None:
        self._producer = confluent_kafka.Producer({
            'bootstrap.servers': server_list(
                self.transport.url, self.transport.default_port),
            'client.id': self.transport.app.conf.broker_client_id,
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
        _size = producer.__len__
        _flush = producer.flush
        _poll = producer.poll
        _sleep = self.sleep
        _create_future = self.loop.create_future
        while not self.should_stop:
            if not _size():
                flush_soon = self._flush_soon
                if flush_soon is None:
                    flush_soon = self._flush_soon = _create_future()
                stopped: bool = False
                try:
                    stopped = await self.wait_for_stopped(
                        flush_soon, timeout=1.0)
                finally:
                    self._flush_soon = None
                if not stopped:
                    _flush(timeout=100)
                    _poll(timeout=0)
                    await _sleep(0)


class ProducerProduceFuture(asyncio.Future):

    def set_from_on_delivery(self,
                             err: Optional[BaseException],
                             msg: _Message) -> None:
        if err:
            # XXX Not sure what err' is here, hopefully it's an exception
            # object and not a string [ask].
            self.set_exception(err)
        else:
            metadata: RecordMetadata = self.message_to_metadata(msg)
            self.set_result(metadata)

    def message_to_metadata(self, message: _Message) -> RecordMetadata:
        topic, partition = tp = TP(message.topic(), message.partition())
        return RecordMetadata(topic, partition, tp, message.offset())


class Producer(base.Producer):
    """Kafka producer using :pypi:`confluent-kafka`."""

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
        _retention = (
            int(want_seconds(retention) * 1000.0)
            if retention else None
        )
        await cast(Transport, self.transport)._create_topic(
            self, topic, partitions, replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=_retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    async def on_start(self) -> None:
        await self._producer_thread.start()
        await self.sleep(0.5)

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
        fut = await self.send(topic, key, value, partition)
        return await fut

    def key_partition(self, topic: str, key: bytes) -> TP:
        raise NotImplementedError()


class Transport(base.Transport):
    """Kafka transport using :pypi:`confluent-kafka`."""

    Consumer: ClassVar[Type[ConsumerT]] = Consumer
    Producer: ClassVar[Type[ProducerT]] = Producer

    default_port = 9092
    if confluent_kafka is not None:
        version = '-'.join(map(str, confluent_kafka.version()))
        libversion = '-'.join(map(str, confluent_kafka.libversion()))
        driver_version = f'confluent_kafka={version} librdkafka={libversion}'
    else:
        version = 'N/A'
        libversion = 'N/A'
        driver_version = 'N/A'

    _topic_waiters: MutableMapping[str, StampedeWrapper]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if confluent_kafka is None:
            raise ImproperlyConfigured(
                '`pip install confluent-kafka` is required for this transport')
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
                owner, topic, partitions, replication,
                loop=self.loop, **kwargs)
        try:
            await wrap()
        except Exception as exc:
            self._topic_waiters.pop(topic, None)
            raise

    async def _really_create_topic(self,
                                   owner: Service,
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
        owner.log.info(f'Creating topic {topic}')  # XXX needs implementation
