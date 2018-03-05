"""Message transport using :pypi:`aiokafka`."""
import asyncio
from itertools import cycle
from typing import (
    Any, AsyncIterator, Awaitable, ClassVar, Iterable,
    Mapping, MutableMapping, Optional, Set, Tuple, Type, Union, cast,
)

import aiokafka
import aiokafka.abc
from aiokafka.errors import (
    ConsumerStoppedError, CommitFailedError,
    IllegalStateError,
)
from aiokafka.structs import (
    OffsetAndMetadata,
    TopicPartition as _TopicPartition,
)
from kafka.errors import (
    NotControllerError, TopicAlreadyExistsError as TopicExistsError, for_code,
)
from mode import Seconds, Service, want_seconds
from mode.utils.futures import StampedeWrapper
from yarl import URL

from . import base
from ..exceptions import ProducerSendError
from ..types import AppT, Message, RecordMetadata, TP
from ..types.transports import ConsumerT, ProducerT
from ..utils.kafka.protocol.admin import CreateTopicsRequest
from ..utils.termtable import logtable

__all__ = ['Consumer', 'Producer', 'Transport']

_TPTypes = Union[TP, _TopicPartition]


def server_list(url: URL, default_port: int) -> str:
    # remove the scheme
    servers = str(url).split('://', 1)[1]
    # add default ports
    return ';'.join(
        host if ':' in host else f'{host}:{default_port}'
        for host in servers.split(';')
    )


def _ensure_TP(tp: _TPTypes) -> TP:
    return tp if isinstance(tp, TP) else TP(tp.topic, tp.partition)


class ConsumerRebalanceListener(aiokafka.abc.ConsumerRebalanceListener):
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
        consumer = cast(Consumer, self.consumer)
        _assigned = set(assigned)
        # remove recently revoked tps from set of paused tps.
        consumer._paused_partitions.intersection_update(_assigned)
        # cache set of assigned partitions
        cast(Set[TP], consumer._set_active_tps(_assigned))
        # start callback chain of assigned callbacks.
        #   need to copy set at this point, since we cannot have
        #   the callbacks mutate our active list.
        await consumer.on_partitions_assigned(_assigned)

    async def on_partitions_revoked(
            self, revoked: Iterable[_TopicPartition]) -> None:
        # see comment in on_partitions_assigned
        consumer = cast(Consumer, self.consumer)
        _revoked = cast(Set[TP], set(revoked))
        # remove revoked partitions from active + paused tps.
        consumer._active_partitions.difference_update(_revoked)
        consumer._paused_partitions.difference_update(_revoked)
        # start callback chain of assigned callbacks.
        await consumer.on_partitions_revoked(set(_revoked))


class Consumer(base.Consumer):
    """Kafka consumer using :pypi:`aiokafka`."""

    RebalanceListener: ClassVar[Type[ConsumerRebalanceListener]]
    RebalanceListener = ConsumerRebalanceListener

    _consumer: aiokafka.AIOKafkaConsumer
    _rebalance_listener: ConsumerRebalanceListener
    _active_partitions: Set[_TopicPartition] = None
    _paused_partitions: Set[_TopicPartition] = None
    _partitions_lock: asyncio.Lock = None
    fetch_timeout: float = 10.0
    wait_for_shutdown = True

    consumer_stopped_errors: ClassVar[Tuple[Type[BaseException], ...]] = (
        ConsumerStoppedError,
    )

    def on_init(self) -> None:
        app = self.transport.app
        transport = cast(Transport, self.transport)
        self._rebalance_listener = self.RebalanceListener(self)
        if app.client_only:
            self._consumer = self._create_client_consumer(app, transport)
        else:
            self._consumer = self._create_worker_consumer(app, transport)
        self._paused_partitions = set()
        self._partitions_lock = asyncio.Lock(loop=self.loop)

    async def on_restart(self) -> None:
        self.on_init()

    def _get_active_partitions(self) -> Set[_TopicPartition]:
        tps = self._active_partitions
        if tps is None:
            # need aiokafka._TopicPartition, not faust.TP
            return self._set_active_tps(self._consumer.assignment())
        return tps

    def _set_active_tps(
            self, tps: Set[_TopicPartition]) -> Set[_TopicPartition]:
        tps = self._active_partitions = set(tps)  # copy!
        tps.difference_update(self._paused_partitions)
        return tps

    def _create_worker_consumer(
            self,
            app: AppT,
            transport: 'Transport') -> aiokafka.AIOKafkaConsumer:
        self._assignor = self.app.assignor
        return aiokafka.AIOKafkaConsumer(
            loop=self.loop,
            client_id=app.client_id,
            group_id=app.id,
            bootstrap_servers=server_list(
                transport.url, transport.default_port),
            partition_assignment_strategy=[self._assignor],
            enable_auto_commit=False,
            auto_offset_reset='earliest',
        )

    def _create_client_consumer(
            self,
            app: AppT,
            transport: 'Transport') -> aiokafka.AIOKafkaConsumer:
        return aiokafka.AIOKafkaConsumer(
            loop=self.loop,
            client_id=app.client_id,
            bootstrap_servers=server_list(
                transport.url, transport.default_port),
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
            topics=set(topics),
            listener=self._rebalance_listener,
        )

    async def getmany(
            self,
            timeout: float) -> AsyncIterator[Tuple[TP, Message]]:
        _consumer = self._consumer
        active_partitions = self._get_active_partitions()

        records: Mapping[TP, Iterable[Message]] = {}
        async with self._partitions_lock:
            if active_partitions:
                # Fetch records only if active partitions to avoid the risk of
                # fetching all partitions in the beginning when none of the
                # partitions is paused/resumed.
                records = await _consumer._fetcher.fetched_records(
                    active_partitions, timeout,
                    max_records=_consumer._max_poll_records,
                )
            else:
                # We should still release to the event loop
                await self.sleep(0)
        create_message = Message  # localize

        iterators = []
        for tp, messages in records.items():
            if tp not in active_partitions:
                self.log.error(f'SKIP PAUSED PARTITION: {tp} '
                               f'ACTIVES: {active_partitions}')
                continue
            iterators.append((
                tp,
                (create_message(
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
                    tp)
                 for message in messages),
            ))

        sentinel = object()
        all: Set[TP] = set(records)
        empty: Set[TP] = set()
        for tp, it in cycle(iterators):
            message: Union[Message, object] = next(it, sentinel)
            if message is sentinel:
                empty.add(tp)
            else:
                yield tp, cast(Message, message)
            if len(all) == len(empty):
                break

    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        return cast(TP, _TopicPartition(topic, partition))

    def _new_offsetandmetadata(self, offset: int, meta: str) -> Any:
        return OffsetAndMetadata(offset, meta)

    async def on_stop(self) -> None:
        await self.commit()
        await self._consumer.stop()
        cast(Transport, self.transport)._topic_waiters.clear()

    async def perform_seek(self) -> None:
        await self.transition_with(base.CONSUMER_SEEKING, self._perform_seek())

    async def _perform_seek(self) -> None:
        read_offset = self._read_offset
        self._consumer.seek_to_committed()
        tps = self._consumer.assignment()
        wait_res = await self.wait(asyncio.gather(*[
            self._consumer.committed(tp) for tp in tps
        ]))
        offsets = zip(tps, wait_res.result)
        committed_offsets = dict(filter(lambda x: x[1] is not None, offsets))
        read_offset.update(committed_offsets)
        self._committed_offset.update(committed_offsets)

    async def _commit(self, offsets: Mapping[TP, Tuple[int, str]]) -> bool:
        table = logtable(
            [(str(tp), str(offset), meta) for tp, (offset, meta) in
             offsets.items()],
            title='Commit Offsets',
            headers=['TP', 'Offset', 'Metadata'],
        )
        self.log.dev('COMMITTING OFFSETS:\n%s', table)
        try:
            await self._consumer.commit({
                tp: self._new_offsetandmetadata(offset, meta)
                for tp, (offset, meta) in offsets.items()
            })
            self._committed_offset.update({
                tp: offset
                for tp, (offset, _) in offsets.items()
            })
            return True
        except CommitFailedError as exc:
            self.log.exception(f'Committing raised exception: %r', exc)
            return False
        except IllegalStateError as exc:
            self.log.exception(f'Got exception: {exc}\n'
                               f'Current assignment: {self.assignment()}')
            await self.crash(exc)
            return False

    async def pause_partitions(self, tps: Iterable[TP]) -> None:
        self.log.info(f'Waiting for lock to pause partitions')
        async with self._partitions_lock:
            self.log.info(f'Acquired lock to pause partitions')
            tpset = set(tps)
            self._get_active_partitions().difference_update(tpset)
            self._paused_partitions.update(tpset)
        self.log.info(f'Released pause partitions lock')

    async def resume_partitions(self, tps: Iterable[TP]) -> None:
        self.log.info(f'Waiting for lock to resume partitions')
        async with self._partitions_lock:
            self.log.info(f'Acquired lock to resume partitions')
            tpset = set(tps)
            self._get_active_partitions().update(tps)
            self._paused_partitions.difference_update(tpset)
        self.log.info(f'Released resume partitions lock')

    async def position(self, tp: TP) -> Optional[int]:
        return await self._consumer.position(tp)

    async def _seek_to_beginning(self, *partitions: TP) -> None:
        self.log.dev('SEEK TO BEGINNING: %r', partitions)
        self._read_offset.update((_ensure_TP(tp), None) for tp in partitions)
        await self._consumer.seek_to_beginning(*(
            self._new_topicpartition(tp.topic, tp.partition)
            for tp in partitions
        ))

    async def seek(self, partition: TP, offset: int) -> None:
        self.log.dev('SEEK %r -> %r', partition, offset)
        self._read_offset[_ensure_TP(partition)] = offset
        self._consumer.seek(partition, offset)

    def assignment(self) -> Set[TP]:
        return cast(Set[TP], self._consumer.assignment())

    def highwater(self, tp: TP) -> int:
        return self._consumer.highwater(tp)

    async def earliest_offsets(self,
                               *partitions: TP) -> MutableMapping[TP, int]:
        return await self._consumer.beginning_offsets(partitions)

    async def highwaters(self, *partitions: TP) -> MutableMapping[TP, int]:
        return await self._consumer.end_offsets(partitions)


class Producer(base.Producer):
    """Kafka producer using :pypi:`aiokafka`."""

    _producer: aiokafka.AIOKafkaProducer

    def on_init(self) -> None:
        transport = cast(Transport, self.transport)
        self._producer = aiokafka.AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=server_list(
                transport.url, transport.default_port),
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
        try:
            return cast(Awaitable[RecordMetadata], await self._producer.send(
                topic, value, key=key, partition=partition))
        except KafkaError as exc:
            raise ProducerSendError(f'Error while sending: {exc}')

    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes],
            partition: Optional[int]) -> RecordMetadata:
        fut = await self.send(topic, key=key, value=value, partition=partition)
        return await fut

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


class Transport(base.Transport):
    """Kafka transport using :pypi:`aiokafka`."""

    Consumer: ClassVar[Type[ConsumerT]] = Consumer
    Producer: ClassVar[Type[ProducerT]] = Producer

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
