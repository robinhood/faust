"""Message transport using :pypi:`aiokafka`."""
import asyncio
import typing
from collections import defaultdict
from enum import Enum
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import aiokafka
import aiokafka.abc
from aiokafka.errors import (
    CommitFailedError,
    ConsumerStoppedError,
    IllegalStateError,
)
from aiokafka.structs import (
    ConsumerRecord,
    OffsetAndMetadata,
)
from rhkafka.errors import (
    NotControllerError,
    TopicAlreadyExistsError as TopicExistsError,
    for_code,
)
from rhkafka.protocol.metadata import MetadataRequest_v1
from mode import Service, flight_recorder, get_logger
from mode.threads import MethodQueue, QueueServiceThread
from mode.utils.compat import OrderedDict
from mode.utils.futures import StampedeWrapper, notify
from mode.utils.locks import Event
from mode.utils.times import Seconds, want_seconds
from yarl import URL

from faust.exceptions import ConsumerNotStarted, ProducerSendError
from faust.transport import base
from faust.types import AppT, ConsumerMessage, Message, RecordMetadata, TP
from faust.types.transports import ConsumerT, ProducerT
from faust.utils import terminal
from faust.utils.kafka.protocol.admin import CreateTopicsRequest

import confluent_kafka
from confluent_kafka import TopicPartition as _TopicPartition
from confluent_kafka import KafkaException

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


# This is what we get from aiokafka getmany()
# A mapping of TP to buffer-list of records.
RecordMap = Mapping[TP, List[ConsumerRecord]]

# But we want to process records from topics in round-robin order.
# We convert records into a mapping from topic-name to "chain-of-buffers":
#   topic_index['topic-name'] = chain(all_topic_partition_buffers)
# This means we can get the next message available in any topic
# by doing: next(topic_index['topic_name'])
TopicIndexMap = MutableMapping[str, '_TopicBuffer']

_TPTypes = Union[TP, _TopicPartition]

logger = get_logger(__name__)


def server_list(urls: List[URL], default_port: int) -> str:
    default_host = '127.0.0.1'
    return ','.join([
        f'{u.host or default_host}:{u.port or default_port}' for u in urls])


def _ensure_TP(tp: _TPTypes) -> TP:
    return tp if isinstance(tp, TP) else TP(tp.topic, tp.partition)


def _ensure_TPset(tps: Iterable[_TPTypes]) -> Set[TP]:
    return {_ensure_TP(tp) for tp in tps}


class _TopicBuffer(Iterator):
    _buffers: Dict[TP, Iterator[ConsumerRecord]]
    _it: Optional[Iterator[ConsumerRecord]]

    def __init__(self) -> None:
        # note: this is a regular dict, but ordered on Python 3.6
        # we use this alias to signify it must be ordered.
        self._buffers = OrderedDict()
        # getmany calls next(_TopicBuffer), and does not call iter(),
        # so the first call to next caches an iterator.
        self._it = None

    def add(self, tp: TP, buffer: List[ConsumerRecord]) -> None:
        assert tp not in self._buffers
        self._buffers[tp] = iter(buffer)

    def __iter__(self) -> Iterator[Tuple[TP, ConsumerRecord]]:
        buffers = self._buffers
        buffers_items = buffers.items
        buffers_remove = buffers.pop
        sentinel = object()
        to_remove: Set[TP] = set()
        mark_as_to_remove = to_remove.add
        while buffers:
            for tp in to_remove:
                buffers_remove(tp, None)
            for tp, buffer in buffers_items():
                item = next(buffer, sentinel)
                if item is sentinel:
                    mark_as_to_remove(tp)
                    continue
                yield tp, item

    def __next__(self) -> Tuple[TP, ConsumerRecord]:
        # Note: this method is not in normal iteration
        # as __iter__ returns generator.
        it = self._it
        if it is None:
            it = self._it = iter(self)
        return it.__next__()


class ConsumerRebalanceListener:
    # kafka's ridiculous class based callback interface makes this hacky.

    def __init__(self, thread: 'ConsumerThread') -> None:
        self._thread: 'ConsumerThread' = thread

    async def on_partitions_revoked(
            self, revoked: Iterable[_TopicPartition]) -> None:
        await self._thread.on_partitions_revoked(
            {TP(tp.topic, tp.partition) for tp in revoked})

    async def on_partitions_assigned(
            self, assigned: Iterable[_TopicPartition]) -> None:
        await self._thread.on_partitions_assigned(
            {TP(tp.topic, tp.partition) for tp in assigned})


class Consumer(base.Consumer):
    """Kafka consumer using :pypi:`aiokafka`."""
    logger = logger

    RebalanceListener: ClassVar[Type[ConsumerRebalanceListener]]
    RebalanceListener = ConsumerRebalanceListener

    _thread: 'ConsumerThread'
    _active_partitions: Optional[Set[TP]]
    _paused_partitions: Set[TP]
    fetch_timeout: float = 10.0

    consumer_stopped_errors: ClassVar[Tuple[Type[BaseException], ...]] = (
        ConsumerStoppedError,
    )

    flow_active: bool = True
    can_resume_flow: Event

    #: Main thread method queue.
    #: The consumer is running in a separate thread, and so we send
    #: requests to it via a queue.
    #: Sometimes the thread needs to call code owned by the main thread,
    #: such as App.on_partitions_revoked, and in that case the thread
    #: uses this method queue.
    _method_queue: MethodQueue

    def on_init(self) -> None:
        self._active_partitions = None
        self._paused_partitions = set()
        self.can_resume_flow = Event()
        self._method_queue = MethodQueue(loop=self.loop, beacon=self.beacon)
        self._thread = ConsumerThread(
            self,
            loop=self.loop,
            beacon=self.beacon,
        )

    async def on_restart(self) -> None:
        self.on_init()

    def _get_active_partitions(self) -> Set[_TopicPartition]:
        tps = self._active_partitions
        if tps is None:
            return self._set_active_tps(self.assignment())
        assert all(isinstance(x, TP) for x in tps)
        return tps

    def _set_active_tps(self, tps: Set[TP]) -> Set[TP]:
        xtps = self._active_partitions = _ensure_TPset(tps)  # copy
        xtps.difference_update(self._paused_partitions)
        return xtps

    def _create_consumer(
            self,
            loop: asyncio.AbstractEventLoop) -> _Consumer:
        transport = cast(Transport, self.transport)
        if self.app.client_only:
            return self._create_client_consumer(transport, loop=loop)
        else:
            return self._create_worker_consumer(transport, loop=loop)

    def _create_worker_consumer(
            self,
            transport: 'Transport',
            loop: asyncio.AbstractEventLoop) -> _Consumer:
        conf = self.app.conf
        self._assignor = self.app.assignor

        # XXX parition.assignment.strategy is string
        # need to write C wrapper for this
        # 'partition.assignment.strategy': [self._assignor]
        return confluent_kafka.Consumer({
            'bootstrap.servers': server_list(
                transport.url, transport.default_port),
            'group.id': conf.id,
            'client.id': conf.broker_client_id,
            'default.topic.config': {
                'auto.offset.reset': 'earliest',
            },
            'enable.auto.commit': False,
            'fetch.max.bytes': conf.consumer_max_fetch_size,
            'request.timeout.ms': int(conf.broker_request_timeout * 1000.0),
            'check.crcs': conf.broker_check_crcs,
            'session.timeout.ms': int(conf.broker_session_timeout * 1000.0),
            'heartbeat.interval.ms': int(
                conf.broker_heartbeat_interval * 1000.0),
        })

    def _create_client_consumer(
            self,
            transport: 'Transport',
            loop: asyncio.AbstractEventLoop) -> aiokafka.AIOKafkaConsumer:
        conf = self.app.conf
        return confluent_kafka.Consumer({
            'bootstrap.servers': server_list(
                transport.url, transport.default_port),
            'client.id': conf.broker_client_id,
            'enable.auto.commit': True,
            'default.topic.config': {
                'auto.offset.reset': 'earliest',
            },
        })

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
        return  # XXX
        await self._thread.create_topic(
            topic,
            partitions,
            replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=int(want_seconds(retention) * 1000.0),
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    async def on_start(self) -> None:
        await self.add_runtime_dependency(self._method_queue)
        await self.add_runtime_dependency(self._thread)

    async def threadsafe_partitions_revoked(
            self,
            receiver_loop: asyncio.AbstractEventLoop,
            revoked: Set[TP]) -> None:
        promise = await self._method_queue.call(
            receiver_loop.create_future(),
            self.on_partitions_revoked,
            revoked,
        )
        # wait for main-thread to finish processing request
        await promise

    async def threadsafe_partitions_assigned(
            self,
            receiver_loop: asyncio.AbstractEventLoop,
            assigned: Set[TP]) -> None:
        promise = await self._method_queue.call(
            receiver_loop.create_future(),
            self.on_partitions_assigned,
            assigned,
        )
        # wait for main-thread to finish processing request
        await promise

    async def subscribe(self, topics: Iterable[str]) -> None:
        await self._thread.subscribe(topics=topics)

    async def getmany(self,
                      timeout: float) -> AsyncIterator[Tuple[TP, Message]]:
        if not self.flow_active:
            await self.wait(self.can_resume_flow)
        # Implementation for the Fetcher service.
        active_partitions = self._get_active_partitions()
        _next = next

        records: RecordMap = {}
        if active_partitions:
            # Fetch records only if active partitions to avoid the risk of
            # fetching all partitions in the beginning when none of the
            # partitions is paused/resumed.
            records = await self._thread.getmany(
                active_partitions,
                timeout=timeout,
            )
        else:
            # We should still release to the event loop
            await self.sleep(1)
            if self.should_stop:
                return
        create_message = ConsumerMessage  # localize

        # records' contain mapping from TP to list of messages.
        # if there are two agents, consuming from topics t1 and t2,
        # normal order of iteration would be to process each
        # tp in the dict:
        #    for tp. messages in records.items():
        #        for message in messages:
        #           yield tp, message
        #
        # The problem with this, is if we have prefetched 16k records
        # for one partition, the other partitions won't even start processing
        # before those 16k records are completed.
        #
        # So we try round-robin between the tps instead:
        #
        #    iterators: Dict[TP, Iterator] = {
        #        tp: iter(messages)
        #        for tp, messages in records.items()
        #    }
        #    while iterators:
        #        for tp, messages in iterators.items():
        #            yield tp, next(messages)
        #            # remove from iterators if empty.
        #
        # The problem with this implementation is that
        # the records mapping is ordered by TP, so records.keys()
        # will look like this:
        #
        #  TP(topic='bar', partition=0)
        #  TP(topic='bar', partition=1)
        #  TP(topic='bar', partition=2)
        #  TP(topic='bar', partition=3)
        #  TP(topic='foo', partition=0)
        #  TP(topic='foo', partition=1)
        #  TP(topic='foo', partition=2)
        #  TP(topic='foo', partition=3)
        #
        # If there are 100 partitions for each topic,
        # it will process 100 items in the first topic, then 100 items
        # in the other topic, but even worse if partition counts
        # vary greatly, t1 has 1000 partitions and t2
        # has 1 partition, then t2 will end up being starved most of the time.
        #
        # We solve this by going round-robin through each topic.
        topic_index = self._records_to_topic_index(records, active_partitions)
        to_remove: Set[str] = set()
        sentinel = object()
        while topic_index:
            if not self.flow_active:
                break
            for topic in to_remove:
                topic_index.pop(topic, None)
            for topic, messages in topic_index.items():
                if not self.flow_active:
                    break
                item = _next(messages, sentinel)
                if item is sentinel:
                    # this topic is now empty,
                    # but we cannot remove from dict while iterating over it,
                    # so move that to the outer loop.
                    to_remove.add(topic)
                    continue
                tp, record = item  # type: ignore
                if tp in active_partitions:
                    highwater_mark = self._thread.highwater(tp)
                    self.app.monitor.track_tp_end_offset(tp, highwater_mark)
                    # convert timestamp to seconds from int milliseconds.
                    timestamp_type: int
                    timestamp: Optional[int]
                    timestamp_type, timestamp = record.timestamp()
                    timestamp_s: float = cast(float, None)
                    if timestamp is not None:
                        timestamp_s = timestamp / 1000.0
                    key = record.key()
                    key_size = len(key) if key is not None else 0
                    value = record.value()
                    value_size = len(value) if value is not None else 0
                    message = create_message(
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        timestamp_s,
                        timestamp_type,
                        key,
                        value,
                        None,
                        key_size,
                        value_size,
                        tp,
                    )
                    yield tp, message

    def _records_to_topic_index(
            self,
            records: RecordMap,
            active_partitions: Set[TP]) -> TopicIndexMap:
        topic_index: TopicIndexMap = {}
        for tp, messages in records.items():
            try:
                entry = topic_index[tp.topic]
            except KeyError:
                entry = topic_index[tp.topic] = _TopicBuffer()
            entry.add(tp, messages)
        return topic_index

    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        return cast(TP, _TopicPartition(topic, partition))

    def _new_offsetandmetadata(self, offset: int, meta: str) -> Any:
        return OffsetAndMetadata(offset, meta)

    async def on_stop(self) -> None:
        await super().on_stop()  # wait_empty
        await self.commit()
        transport = cast(Transport, self.transport)
        transport._topic_waiters.clear()

    async def perform_seek(self) -> None:
        read_offset = self._read_offset
        _committed_offsets = await self._thread.seek_to_committed()
        committed_offsets = {
            _ensure_TP(tp): offset
            for tp, offset in _committed_offsets.items()
            if offset is not None
        }
        read_offset.update({
            tp: offset if offset else None
            for tp, offset in committed_offsets.items()
        })
        self._committed_offset.update(committed_offsets)

    async def _commit(self, offsets: Mapping[TP, Tuple[int, str]]) -> bool:
        table = terminal.logtable(
            [(str(tp), str(offset), meta)
             for tp, (offset, meta) in offsets.items()],
            title='Commit Offsets',
            headers=['TP', 'Offset', 'Metadata'],
        )
        self.log.dev('COMMITTING OFFSETS:\n%s', table)
        try:
            assignment = self.assignment()
            revoked: Dict[TP, int] = {}
            commitable_offsets: Dict[TP, int] = {}
            for tp, (offset, _) in offsets.items():
                if tp in assignment:
                    commitable_offsets[tp] = offset
                else:
                    revoked[tp] = offset
            if revoked:
                self.log.info(
                    'Discarded commit for revoked partitions that '
                    'will be eventually processed again: %r',
                    revoked,
                )
            if not commitable_offsets:
                return False
            with flight_recorder(self.log, timeout=300.0) as on_timeout:
                on_timeout.info('+confluent_consumer.commit()')
                await self._thread.commit(commitable_offsets)
                on_timeout.info('-confluent_consumer.commit()')
            self._committed_offset.update(commitable_offsets)
            self.app.monitor.on_tp_commit(commitable_offsets)
            self._last_batch = None
            return True
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

    def stop_flow(self) -> None:
        self.flow_active = False
        self.can_resume_flow.clear()

    def resume_flow(self) -> None:
        self.flow_active = True
        self.can_resume_flow.set()

    def pause_partitions(self, tps: Iterable[TP]) -> None:
        tpset = _ensure_TPset(tps)
        self._get_active_partitions().difference_update(tpset)
        self._paused_partitions.update(tpset)

    def resume_partitions(self, tps: Iterable[TP]) -> None:
        tpset = _ensure_TPset(tps)
        self._get_active_partitions().update(tps)
        self._paused_partitions.difference_update(tpset)

    async def position(self, tp: TP) -> Optional[int]:
        return await self._thread.position(tp)

    async def seek_wait(self, partitions: Mapping[TP, int]) -> None:
        return await self._thread.seek_wait(partitions)

    async def _seek_to_beginning(self, *partitions: TP) -> None:
        self.log.dev('SEEK TO BEGINNING: %r', partitions)
        self._read_offset.update((_ensure_TP(tp), None) for tp in partitions)
        await self._thread.seek_to_beginning(*(
            self._new_topicpartition(tp.topic, tp.partition)
            for tp in partitions
        ))

    async def seek(self, partition: TP, offset: int) -> None:
        self.log.dev('SEEK %r -> %r', partition, offset)
        # reset livelock detection
        self._last_batch = None
        # set new read offset so we will reread messages
        self._read_offset[_ensure_TP(partition)] = offset if offset else None
        self._thread.seek(partition, offset)

    def assignment(self) -> Set[TP]:
        return self._thread.assignment()

    def highwater(self, tp: TP) -> int:
        return self._thread.highwater(tp)

    def topic_partitions(self, topic: str) -> Optional[int]:
        return self._thread.topic_partitions(topic)

    async def earliest_offsets(self,
                               *partitions: TP) -> MutableMapping[TP, int]:
        return await self._thread.earliest_offsets(*partitions)

    async def highwaters(self, *partitions: TP) -> MutableMapping[TP, int]:
        return await self._thread.highwaters(*partitions)

    def close(self) -> None:
        self._thread.close()


class ConsumerThread(QueueServiceThread):
    app: AppT
    consumer: Consumer
    _consumer: Optional[_Consumer] = None
    _assigned: bool = False

    def __init__(self, consumer: Consumer, **kwargs: Any) -> None:
        self.consumer = consumer
        transport = self.consumer.transport
        self.app = transport.app
        self._rebalance_listener = consumer.RebalanceListener(self)
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        self._consumer = self.consumer._create_consumer(loop=self.thread_loop)

    def close(self) -> None:
        ...

    async def on_partitions_revoked(
            self, revoked: Iterable[TP]) -> None:
        self.consumer.app.rebalancing = True  # set as early as possible
        # see comment in on_partitions_assigned
        consumer = self.consumer
        _revoked = _ensure_TPset(revoked)
        # remove revoked partitions from active + paused tps.
        if consumer._active_partitions is not None:
            consumer._active_partitions.difference_update(_revoked)
        consumer._paused_partitions.difference_update(_revoked)
        # start callback chain of assigned callbacks.
        await consumer.threadsafe_partitions_revoked(
            self.thread_loop, _revoked)

    async def on_partitions_assigned(
            self, assigned: Iterable[_TopicPartition]) -> None:
        # have to cast to Consumer since ConsumerT interface does not
        # have this attribute (mypy currently thinks a Callable instance
        # variable is an instance method).  Furthermore we have to cast
        # the Kafka TopicPartition namedtuples to our description,
        # that way they are typed and decoupled from the actual client
        # implementation.
        consumer = self.consumer
        _assigned = _ensure_TPset(assigned)
        # remove recently revoked tps from set of paused tps.
        consumer._paused_partitions.intersection_update(_assigned)
        # cache set of assigned partitions
        consumer._set_active_tps(_assigned)
        # start callback chain of assigned callbacks.
        #   need to copy set at this point, since we cannot have
        #   the callbacks mutate our active list.
        consumer._last_batch = None
        await consumer.threadsafe_partitions_assigned(
            self.thread_loop, _assigned)

    async def subscribe(self, topics: Iterable[str]) -> None:
        # XXX pattern does not work :/
        await self.call_thread(
            self._ensure_consumer().subscribe,
            topics=list(topics),
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        while not self._assigned:
            self.log.info('Still waiting for assignment...')
            self._ensure_consumer().poll(timeout=1)

    def _on_assign(self,
                   consumer: _Consumer,
                   assigned: List[_TopicPartition]) -> None:
        self._assigned = True
        self.thread_loop.run_until_complete(
            self._rebalance_listener.on_partitions_assigned(assigned))

    def _on_revoke(self,
                   consumer: _Consumer,
                   revoked: List[_TopicPartition]) -> None:
        self.thread_loop.run_until_complete(
            self._rebalance_listener.on_partitions_revoked(revoked))

    async def seek_to_committed(self) -> Mapping[TP, int]:
        return await self.call_thread(self._seek_to_committed)

    async def _seek_to_committed(self) -> Mapping[TP, int]:
        consumer = self._ensure_consumer()
        assignment = consumer.assignment()
        committed = consumer.committed(assignment)
        for tp in committed:
            consumer.seek(tp)
        return {_ensure_TP(tp): tp.offset for tp in committed}

    async def _committed_offsets(
            self, partitions: List[TP]) -> MutableMapping[TP, int]:
        consumer = self._ensure_consumer()
        committed = consumer.committed(
            [_TopicPartition(tp[0], tp[1]) for tp in partitions])
        return {
            TP(tp.topic, tp.partition): tp.offset
            for tp in committed
        }

    async def commit(self, tps: Mapping[TP, int]) -> Any:
        return await self.call_thread(
            self._ensure_consumer().commit,
            offsets=[
                _TopicPartition(tp.topic, tp.partition, offset=offset)
                for tp, offset in tps.items()
            ],
            asynchronous=False,
        )

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
        return _ensure_TPset(self._ensure_consumer().assignment())

    def highwater(self, tp: TP) -> int:
        _, hw = self._ensure_consumer().get_watermark_offsets(
            _TopicPartition(tp.topic, tp.partition), cached=True)
        return hw

    def topic_partitions(self, topic: str) -> Optional[int]:
        # XXX NotImplemented
        return None

    async def earliest_offsets(self,
                               *partitions: TP) -> MutableMapping[TP, int]:
        if not partitions:
            return {}
        return await self.call_thread(self._earliest_offsets, partitions)

    async def _earliest_offsets(
            self, partitions: List[TP]) -> MutableMapping[TP, int]:
        consumer = self._ensure_consumer()
        return {
            tp: consumer.get_watermark_offsets(
                _TopicPartition(tp[0], tp[1]))[0]
            for tp in partitions
        }

    async def highwaters(self, *partitions: TP) -> MutableMapping[TP, int]:
        if not partitions:
            return {}
        return await self.call_thread(self._highwaters, partitions)

    async def _highwaters(
            self, partitions: List[TP]) -> MutableMapping[TP, int]:
        consumer = self._ensure_consumer()
        return {
            tp: consumer.get_watermark_offsets(
                _TopicPartition(tp[0], tp[1]))[1]
            for tp in partitions
        }

    def _ensure_consumer(self) -> aiokafka.AIOKafkaConsumer:
        if self._consumer is None:
            raise ConsumerNotStarted('Consumer thread not yet started')
        return self._consumer

    async def getmany(self,
                      active_partitions: Set[TP],
                      timeout: float) -> RecordMap:
        # Implementation for the Fetcher service.
        _consumer = self._ensure_consumer()
        messages = await self.call_thread(
            _consumer.consume,
            num_messages=10000,
            timeout=timeout,
        )
        records: RecordMap = defaultdict(list)
        for message in messages:
            tp = TP(message.topic(), message.partition())
            records[tp].append(message)
        return records

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
        consumer = self.consumer
        transport = cast(Transport, consumer.transport)
        _consumer = self._ensure_consumer()
        await self.call_thread(
            transport._create_topic,
            consumer,
            _consumer._client,
            topic,
            partitions,
            replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=int(want_seconds(retention) * 1000.0),
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )


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


class ProducerThread(QueueServiceThread):
    app: AppT
    producer: 'Producer'
    transport: 'Transport'
    _producer: Optional[_Producer] = None
    _flush_soon: Optional[asyncio.Future] = None

    def __init__(self, producer: 'Producer', **kwargs: Any) -> None:
        self.producer = producer
        self.transport = cast(Transport, self.producer.transport)
        self.app = self.transport.app
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        self._producer = confluent_kafka.Producer({
            'bootstrap.servers': server_list(
                self.transport.url, self.transport.default_port),
            'client.id': self.app.conf.broker_client_id,
            'max.in.flight.requests.per.connection': 1,
        })

    async def flush(self) -> None:
        if self._producer is not None:
            self._producer.flush()

    async def on_thread_stop(self) -> None:
        if self._producer is not None:
            self._producer.flush()

    def produce(self, topic: str, key: bytes, value: bytes, partition: int,
                on_delivery: Callable) -> None:
        if self._producer is None:
            raise RuntimeError('Producer not started')
        if partition is not None:
            self._producer.produce(
                topic, key, value, partition, on_delivery=on_delivery,
            )
        else:
            self._producer.produce(
                topic, key, value, on_delivery=on_delivery,
            )
        notify(self._flush_soon)

    @Service.task
    async def _background_flush(self) -> None:
        producer = cast(_Producer, self._producer)
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
                    _poll(timeout=1)
                    await _sleep(0)


class Producer(base.Producer):
    """Kafka producer using :pypi:`aiokafka`."""

    logger = logger

    _producer_thread: ProducerThread
    _quick_produce: Any = None

    def on_init(self) -> None:
        self._producer_thread = ProducerThread(
            self, loop=self.loop, beacon=self.beacon)
        self._quick_produce = self._producer_thread.produce

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
        return  # XXX
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
        await self._producer_thread.start()
        await self.sleep(0.5)  # cannot remember why, necessary? [ask]
        self._last_batch = None

    async def on_stop(self) -> None:
        cast(Transport, self.transport)._topic_waiters.clear()
        self._last_batch = None
        await self._producer_thread.stop()

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int]) -> Awaitable[RecordMetadata]:
        fut = ProducerProduceFuture(loop=self.loop)
        self._quick_produce(
            topic, value, key, partition,
            on_delivery=fut.set_from_on_delivery,
        )
        return cast(Awaitable[RecordMetadata], fut)
        try:
            return cast(Awaitable[RecordMetadata], await self._producer.send(
                topic, value, key=key, partition=partition))
        except KafkaException as exc:
            raise ProducerSendError(f'Error while sending: {exc!r}') from exc

    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int]) -> RecordMetadata:
        fut = await self.send(topic, key, value, partition)
        return await fut

    async def flush(self) -> None:
        await self._producer_thread.flush()

    def key_partition(self, topic: str, key: bytes) -> TP:
        raise NotImplementedError()


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
                owner,
                client,
                topic,
                partitions,
                replication,
                loop=self.loop, **kwargs)
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
        owner.log.info(f'Found controller: {controller_node}')

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
            owner.log.info(f'Shutting down - skipping creation.')
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
