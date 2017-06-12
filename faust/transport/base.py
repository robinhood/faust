"""Base message transport implementation."""
import abc
import asyncio
from collections import defaultdict
from itertools import count
from typing import (
    Any, AsyncIterator, Awaitable, ClassVar, Iterable, Iterator,
    List, MutableMapping, Optional, Set, Tuple, Type, cast,
)
from ..types import AppT, Message, TopicPartition
from ..types.transports import (
    ConsumerCallback, ConsumerT, ProducerT, TPorTopicSet, TransportT,
    PartitionsAssignedCallback, PartitionsRevokedCallback,
)
from ..utils.functional import consecutive_numbers
from ..utils.logging import get_logger
from ..utils.services import Service, ServiceT

__all__ = ['Consumer', 'Producer', 'Transport']

logger = get_logger(__name__)

# The Transport is responsible for:
#
#  - Holds reference to the app that created it.
#  - Creates new consumers/producers.
#
# The Consumer is responsible for:
#
#   - Holds reference to the transport that created it
#   - ... and the app via ``self.transport.app``.
#   - Has a callback that usually points back to ``TopicManager.on_message``.
#   - Receives messages and calls the callback for every message received.
#   - Keeps track of the message and it's acked/unacked status.
#   - If automatic acks are enabled the message is acked when the Message goes
#     out of scope (like any variable using reference counting).
#   - The TopicManager forwards the message to all Streams that subscribes
#     to the topic the message was sent to.
#      - Each individual Stream will deserialize the message and create
#        one Event instance per stream.  The message goes out of scope (and so
#        acked), when all events referencing the message is also out of scope.
#   - Commits the offset at an interval
#      - The current offset is based on range of the messages acked.
#
# The Producer is responsible for:
#
#   - Holds reference to the transport that created it
#   - ... and the app via ``self.transport.app``.
#   - Sending messages.
#
# To see a reference transport implementation go to:
#     faust/transport/aiokafka.py


class Consumer(Service, ConsumerT):
    """Base Consumer."""
    logger = logger

    RebalanceListener: ClassVar[Type]

    consumer_stopped_errors: ClassVar[Tuple[Type[Exception], ...]] = None

    #: This counter generates new consumer ids.
    _consumer_ids: ClassVar[Iterator[int]] = count(0)

    _app: AppT

    # Mapping of TP to list of acked offsets.
    _acked: MutableMapping[TopicPartition, List[int]] = None

    #: Fast lookup to see if tp+offset was acked.
    _acked_index: MutableMapping[TopicPartition, Set[int]]

    #: Keeps track of the currently commited offset in each TP.
    _current_offset: MutableMapping[TopicPartition, int] = None

    #: Queue for the when-acked-call-sensors background thread.
    _recently_acked: asyncio.Queue

    #: Event set whenever we resubscribe
    _time_to_seek: asyncio.Event

    def __init__(self, transport: TransportT,
                 *,
                 callback: ConsumerCallback = None,
                 on_partitions_revoked: PartitionsRevokedCallback = None,
                 on_partitions_assigned: PartitionsAssignedCallback = None,
                 autoack: bool = True,
                 commit_interval: float = None,
                 **kwargs: Any) -> None:
        assert callback is not None
        self.id = next(self._consumer_ids)
        self.transport = transport
        self._app = self.transport.app
        self.callback = callback
        self.autoack = autoack
        self._on_message_in = self._app.sensors.on_message_in
        self._on_partitions_revoked = on_partitions_revoked
        self._on_partitions_assigned = on_partitions_assigned
        self.commit_interval = (
            commit_interval or self._app.commit_interval)
        self._acked = defaultdict(list)
        self._acked_index = defaultdict(set)
        self._current_offset = defaultdict(int)
        self._commit_mutex = asyncio.Lock(loop=self.loop)
        self._rebalance_listener = self.RebalanceListener(self)
        self._recently_acked = asyncio.Queue(loop=self.transport.loop)
        self._time_to_seek = asyncio.Event(loop=self.transport.loop)
        super().__init__(loop=self.transport.loop, **kwargs)

    @abc.abstractmethod
    async def _perform_seek(self) -> None:
        ...

    @abc.abstractmethod
    async def _commit(self, offsets: Any) -> None:
        ...

    @abc.abstractmethod
    def _get_topic_meta(self, topic: str) -> Any:
        ...

    @abc.abstractmethod
    def _new_topicpartition(
            self, topic: str, partition: int) -> TopicPartition:
        ...

    @abc.abstractmethod
    def _new_offsetandmetadata(self, offset: int, meta: Any) -> Any:
        ...

    def on_partitions_assigned(
            self, assigned: Iterable[TopicPartition]) -> None:
        self._on_partitions_assigned(assigned)
        self._time_to_seek.set()

    def on_partitions_revoked(
            self, revoked: Iterable[TopicPartition]) -> None:
        self._on_partitions_revoked(revoked)

    async def track_message(
            self, message: Message, tp: TopicPartition, offset: int) -> None:
        _id = self.id

        # call sensors
        await self._on_message_in(_id, tp, offset, message)

    def ack(self, tp: TopicPartition, offset: int) -> None:
        if offset > self._current_offset[tp]:
            acked_index = self._acked_index[tp]
            if offset not in acked_index:
                acked_index.add(offset)
                acked_for_tp = self._acked[tp]
                acked_for_tp.append(offset)
                acked_for_tp.sort()
                self._recently_acked.put_nowait((tp, offset))

    @Service.task
    async def _commit_handler(self) -> None:
        await self.sleep(self.commit_interval)
        while not self.should_stop:
            await self.commit()
            await self.sleep(self.commit_interval)

    @Service.task
    async def _recently_acked_handler(self) -> None:
        get = self._recently_acked.get
        on_message_out = self._app.sensors.on_message_out
        while not self.should_stop:
            tp, offset = await get()
            await on_message_out(self.id, tp, offset, None)

    @Service.task
    async def _seeker(self) -> None:
        while not self.should_stop:
            await self._time_to_seek.wait()
            try:
                await self._perform_seek()
            finally:
                self._time_to_seek.clear()

    async def commit(self, topics: TPorTopicSet = None) -> bool:
        """Maybe commit the offset for all or specific topics.

        Keyword Arguments:
            topics: Set containing topics and/or TopicPartitions to commit.
        """
        did_commit = False

        # Only one coroutine can commit at a time.
        async with self._commit_mutex:
            sensor_state = await self._app.sensors.on_commit_initiated(self)

            # Go over the ack list in each topic/partition:
            for tp in self._filter_tps_with_pending_acks(topics):
                # Find the latest offset we can commit in this tp
                offset = self._new_offset(tp)
                # check if we can commit to this offset
                if offset is not None and self._should_commit(tp, offset):
                    # if so, first send all messages attached to the new
                    # offset
                    await self._app.commit_attached(tp, offset)
                    # then, update the current_offset and perform
                    # the commit.
                    self._current_offset[tp] = offset
                    meta = self._get_topic_meta(tp.topic)
                    did_commit = True
                    await self._do_commit(tp, offset, meta)
            await self._app.sensors.on_commit_completed(self, sensor_state)
        return did_commit

    def _filter_tps_with_pending_acks(
            self, topics: TPorTopicSet = None) -> Iterator[TopicPartition]:
        return (
            tp for tp in self._acked
            if topics is None or tp in topics or tp.topic in topics
        )

    def _should_commit(self, tp: TopicPartition, offset: int) -> bool:
        return bool(offset) and offset > self._current_offset[tp]

    def _new_offset(self, tp: TopicPartition) -> Optional[int]:
        # get the new offset for this tp, by going through
        # its list of acked messages.
        acked = self._acked[tp]

        # We iterate over it until we find a gap
        # then return the offset before that.
        # For example if acked[tp] is:
        #   1 2 3 4 5 6 7 8 9
        # the return value will be: 9
        # If acked[tp] is:
        #  34 35 36 40 41 42 43 44
        #          ^--- gap
        # the return value will be: 36
        if acked:
            # Note: acked is always kept sorted.
            # find first list of consecutive numbers
            batch = next(consecutive_numbers(acked))
            # remove them from the list to clean up.
            acked[:len(batch)] = []
            self._acked_index[tp].difference_update(batch)
            # return the highest commit offset
            return batch[-1]
        return None

    async def _do_commit(
            self, tp: TopicPartition, offset: int, meta: Any) -> None:
        await self._commit({tp: self._new_offsetandmetadata(offset, meta)})

    async def on_task_error(self, exc: Exception) -> None:
        if self.autoack:
            await self.commit()

    async def _drain_messages(self) -> None:
        callback = self.callback
        getmany = self.getmany
        track_message = self.track_message
        should_stop = self._stopped.is_set
        get_current_offset = self._current_offset.__getitem__

        async def deliver(message: Message, tp: TopicPartition) -> None:
            await track_message(message, tp, message.offset)
            await callback(message)

        try:
            # XXX mypy confuses AsyncIterable/AsyncIterator
            while not should_stop():
                ait = cast(AsyncIterator, getmany(timeout=1.0))
                async for tp, message in ait:
                    offset = get_current_offset(tp)
                    if offset is None or message.offset > offset:
                        await deliver(message, tp)
        except self.consumer_stopped_errors:
            if self.transport.app.should_stop:
                # we're already stopping so ignore
                self.log.info('Broker stopped consumer, shutting down...')
                return
            raise
        except Exception as exc:
            self.log.exception('Drain messages raised: %r', exc)
            raise
        finally:
            self.set_shutdown()


class Fetcher(Service):
    app: AppT

    def __init__(self, app: AppT, **kwargs) -> None:
        self.app = app
        super().__init__(**kwargs)

    @Service.task
    async def _fetcher(self) -> None:
        await cast(Consumer, self.app.consumer)._drain_messages()


class Producer(Service, ProducerT):
    """Base Producer."""
    logger = logger

    def __init__(self, transport: TransportT, **kwargs: Any) -> None:
        self.transport = transport
        super().__init__(loop=self.transport.loop, **kwargs)

    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes],
            partition: Optional[int]) -> Awaitable:
        raise NotImplementedError()

    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: Optional[bytes],
            partition: Optional[int]) -> Awaitable:
        raise NotImplementedError()


class Transport(TransportT):
    """Message transport implementation."""

    #: Consumer subclass used for this transport.
    Consumer: ClassVar[Type[ConsumerT]]

    #: Producer subclass used for this transport.
    Producer: ClassVar[Type[ProducerT]]

    #: Service that fetches messages from the broker.
    Fetcher: ClassVar[Type[ServiceT]] = Fetcher

    driver_version: str

    def __init__(self, url: str, app: AppT,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.url = url
        self.app = app
        self.loop = loop

    def create_consumer(self, callback: ConsumerCallback,
                        **kwargs: Any) -> ConsumerT:
        return self.Consumer(self, callback=callback, **kwargs)

    def create_producer(self, **kwargs: Any) -> ProducerT:
        return self.Producer(self, **kwargs)
