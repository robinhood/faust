"""Base message transport implementation."""
import abc
import asyncio
import gc
import typing
from collections import defaultdict
from time import monotonic
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    ClassVar,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSet,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)
from weakref import WeakSet

from mode.services import Service, ServiceT
from mode.utils.futures import notify
from yarl import URL

from faust.exceptions import ProducerSendError
from faust.types import AppT, Message, RecordMetadata, TP
from faust.types.transports import (
    ConsumerCallback,
    ConsumerT,
    PartitionsAssignedCallback,
    PartitionsRevokedCallback,
    ProducerT,
    TPorTopicSet,
    TransportT,
)
from faust.utils.functional import consecutive_numbers

if typing.TYPE_CHECKING:
    from faust.app import App
else:
    class App: ...  # noqa

__all__ = ['Consumer', 'Producer', 'Transport']

CONSUMER_FETCHING = 'FETCHING'
CONSUMER_PARTITIONS_REVOKED = 'PARTITIONS_REVOKED'
CONSUMER_PARTITIONS_ASSIGNED = 'PARTITIONS_ASSIGNED'
CONSUMER_COMMITTING = 'COMMITTING'
CONSUMER_SEEKING = 'SEEKING'
CONSUMER_WAIT_EMPTY = 'WAIT_EMPTY'


# The Transport is responsible for:
#
#  - Holds reference to the app that created it.
#  - Creates new consumers/producers.
#
# The Consumer is responsible for:
#
#   - Holds reference to the transport that created it
#   - ... and the app via ``self.transport.app``.
#   - Has a callback that usually points back to ``TopicConductor.on_message``.
#   - Receives messages and calls the callback for every message received.
#   - Keeps track of the message and it's acked/unacked status.
#   - If automatic acks are enabled the message is acked when the Message goes
#     out of scope (like any variable using reference counting).
#   - The TopicConductor forwards the message to all Streams that subscribes
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

    consumer_stopped_errors: ClassVar[Tuple[Type[BaseException], ...]] = None

    app: AppT

    # Mapping of TP to list of acked offsets.
    _acked: MutableMapping[TP, List[int]] = None

    #: Fast lookup to see if tp+offset was acked.
    _acked_index: MutableMapping[TP, Set[int]]

    #: Keeps track of the currently read offset in each TP
    _read_offset: MutableMapping[TP, int]

    #: Keeps track of the currently commited offset in each TP.
    _committed_offset: MutableMapping[TP, int] = None

    #: The consumer.wait_empty() method will set this to be notified
    #: when something acks a message.
    _waiting_for_ack: asyncio.Future = None

    #: Used by .commit to ensure only one thread is comitting at a time.
    #: Other thread starting to commit while a commit is already active,
    #: will wait for the original request to finish, and do nothing.
    _commit_fut: asyncio.Future = None

    #: Set of unacked messages: that is messages that we started processing
    #: and that we MUST attempt to complete processing of, before
    #: shutting down or resuming a rebalance.
    _unacked_messages: MutableSet[Message] = None

    #: Time of last record batch received.
    #: Set only when not set, and reset by commit() so actually
    #: tracks how long it ago it was since we received a record that
    #: was never committed.
    _last_batch: float = None

    #: Time of when the consumer was started.
    _time_start: float = None

    def __init__(self,
                 transport: TransportT,
                 *,
                 callback: ConsumerCallback = None,
                 on_partitions_revoked: PartitionsRevokedCallback = None,
                 on_partitions_assigned: PartitionsAssignedCallback = None,
                 commit_interval: float = None,
                 commit_livelock_soft_timeout: float = None,
                 **kwargs: Any) -> None:
        assert callback is not None
        self.transport = transport
        self.app = self.transport.app
        self.callback = callback
        self._on_message_in = self.app.sensors.on_message_in
        self._on_partitions_revoked = on_partitions_revoked
        self._on_partitions_assigned = on_partitions_assigned
        self.commit_interval = (
            commit_interval or self.app.conf.broker_commit_interval)
        self.commit_livelock_soft_timeout = (
            commit_livelock_soft_timeout or
            self.app.conf.broker_commit_livelock_soft_timeout)
        self._acked = defaultdict(list)
        self._acked_index = defaultdict(set)
        self._read_offset = defaultdict(lambda: None)
        self._committed_offset = defaultdict(lambda: None)
        self._unacked_messages = WeakSet()
        self._waiting_for_ack = None
        self._time_start = monotonic()
        self.randomly_assigned_topics = set()
        super().__init__(loop=self.transport.loop, **kwargs)

    @abc.abstractmethod
    async def _commit(self, offsets: Mapping[TP, Tuple[int, str]]) -> bool:
        ...

    @abc.abstractmethod
    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        ...

    def _is_changelog_tp(self, tp: TP) -> bool:
        return tp.topic in self.app.tables.changelog_topics

    @Service.transitions_to(CONSUMER_PARTITIONS_ASSIGNED)
    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        await self._on_partitions_assigned(assigned)

    @Service.transitions_to(CONSUMER_PARTITIONS_REVOKED)
    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        await self._on_partitions_revoked(revoked)

    async def verify_subscription(self, assigned: Set[TP]) -> None:
        ...

    async def track_message(self, message: Message) -> None:
        # add to set of pending messages that must be acked for graceful
        # shutdown.  This is called by faust.topics.TopicConductor,
        # before delivering messages to streams.
        if message not in self._unacked_messages:
            self._unacked_messages.add(message)

            # call sensors
            await self._on_message_in(message.tp, message.offset, message)

    def ack(self, message: Message) -> bool:
        if not message.acked:
            message.acked = True
            tp = message.tp
            offset = message.offset
            if self.app.topics.acks_enabled_for(message.topic):
                committed = self._committed_offset[tp]
                try:
                    if committed is None or offset > committed:
                        acked_index = self._acked_index[tp]
                        if offset not in acked_index:
                            self._unacked_messages.discard(message)
                            acked_index.add(offset)
                            acked_for_tp = self._acked[tp]
                            acked_for_tp.append(offset)
                            return True
                finally:
                    notify(self._waiting_for_ack)
            else:
                assert message not in self._unacked_messages
        return False

    @Service.transitions_to(CONSUMER_WAIT_EMPTY)
    async def wait_empty(self) -> None:
        """Wait for all messages that started processing to be acked."""
        wait_count = 0
        while not self.should_stop and self._unacked_messages:
            wait_count += 1
            if not wait_count % 100_000:
                remaining = [(m.refcount, m) for m in self._unacked_messages]
                self.log.warn(f'Waiting for {remaining}')
            self.log.dev('STILL WAITING FOR ALL STREAMS TO FINISH')
            gc.collect()
            await self.commit()
            if not self._unacked_messages:
                break

            # arm future so that `ack()` can wake us up
            self._waiting_for_ack = asyncio.Future(loop=self.loop)
            try:
                # wait for `ack()` to wake us up
                asyncio.wait_for(
                    self._waiting_for_ack, loop=self.loop, timeout=1)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            finally:
                self._waiting_for_ack = None
        self.log.dev('COMMITTING AGAIN AFTER STREAMS DONE')
        await self.commit()

    async def on_stop(self) -> None:
        await self.wait_empty()

    @Service.task
    async def _commit_handler(self) -> None:
        await self.sleep(self.commit_interval)
        while not self.should_stop:
            await self.commit()
            await self.sleep(self.commit_interval)

    @Service.task
    async def _commit_livelock_detector(self) -> None:
        # XXX Should take into account the time of last records recieved.
        soft_timeout = self.commit_livelock_soft_timeout
        interval: float = self.commit_interval * 2.5
        await self.sleep(interval)
        while not self.should_stop:
            if self._last_batch is not None:
                s_since_batch = monotonic() - self._last_batch
                if s_since_batch > soft_timeout:
                    self.log.warn(
                        'Possible livelock: COMMIT OFFSET NOT ADVANCING')
            await self.sleep(interval)

    async def commit(self, topics: TPorTopicSet = None) -> bool:
        """Maybe commit the offset for all or specific topics.

        Arguments:
            topics: Set containing topics and/or TopicPartitions to commit.
        """
        # Only one coroutine allowed to commit at a time,
        # and other coroutines should wait for the original commit to finish
        # then do nothing.
        if self._commit_fut is not None:
            # something is already committing so wait for that future.
            try:
                await self._commit_fut
            except asyncio.CancelledError:
                # if future is cancelled we have to start new commit
                pass
            else:
                # original commit finished, return False as we did not commit
                return False

        self._commit_fut = asyncio.Future(loop=self.loop)
        try:
            return await self.force_commit(topics)
        finally:
            # set commit_fut to None so that next call will commit.
            fut, self._commit_fut = self._commit_fut, None
            # notify followers that the commit is done.
            fut.set_result(None)

    @Service.transitions_to(CONSUMER_COMMITTING)
    async def force_commit(self, topics: TPorTopicSet = None) -> bool:
        # Only one coroutine can commit at a time.
        sensor_state = await self.app.sensors.on_commit_initiated(self)

        # Go over the ack list in each topic/partition
        commit_tps = list(self._filter_tps_with_pending_acks(topics))
        did_commit = await self._commit_tps(commit_tps)

        await self.app.sensors.on_commit_completed(self, sensor_state)
        return did_commit

    async def _commit_tps(self, tps: Iterable[TP]) -> bool:
        commit_offsets = {}
        for tp in tps:
            # Find the latest offset we can commit in this tp
            offset = self._new_offset(tp)
            # check if we can commit to this offset
            if offset is not None and self._should_commit(tp, offset):
                commit_offsets[tp] = offset
        if commit_offsets:
            try:
                # send all messages attached to the new offset
                await self._handle_attached(commit_offsets)
            except ProducerSendError as exc:
                await self.crash(exc)
            else:
                return await self._commit_offsets(commit_offsets)
        return False

    async def _handle_attached(self, commit_offsets: Mapping[TP, int]) -> None:
        for tp, offset in commit_offsets.items():
            await cast(App, self.app)._attachments.commit(tp, offset)

    async def _commit_offsets(self, commit_offsets: Mapping[TP, int]) -> bool:
        meta = ''
        return await self._commit({
            tp: (offset, meta)
            for tp, offset in commit_offsets.items()
        })

    def _filter_tps_with_pending_acks(
            self, topics: TPorTopicSet = None) -> Iterator[TP]:
        return (tp for tp in self._acked
                if topics is None or tp in topics or tp.topic in topics)

    def _should_commit(self, tp: TP, offset: int) -> bool:
        committed = self._committed_offset[tp]
        return committed is None or bool(offset) and offset > committed

    def _new_offset(self, tp: TP) -> Optional[int]:
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
            acked.sort()
            # Note: acked is always kept sorted.
            # find first list of consecutive numbers
            batch = next(consecutive_numbers(acked))
            # remove them from the list to clean up.
            acked[:len(batch)] = []
            self._acked_index[tp].difference_update(batch)
            # return the highest commit offset
            return batch[-1]
        return None

    async def on_task_error(self, exc: BaseException) -> None:
        await self.commit()

    async def _drain_messages(self, fetcher: ServiceT) -> None:
        # This is the background thread started by Fetcher, used to
        # constantly read messages using Consumer.getmany.
        # It takes Fetcher as argument, because we must be able to
        # stop it using `await Fetcher.stop()`.
        callback = self.callback
        getmany = self.getmany
        consumer_should_stop = self._stopped.is_set
        fetcher_should_stop = fetcher._stopped.is_set

        get_read_offset = self._read_offset.__getitem__
        set_read_offset = self._read_offset.__setitem__
        flag_consumer_fetching = CONSUMER_FETCHING
        set_flag = self.diag.set_flag
        unset_flag = self.diag.unset_flag

        try:
            while not (consumer_should_stop() or fetcher_should_stop()):
                set_flag(flag_consumer_fetching)
                ait = cast(AsyncIterator, getmany(timeout=5.0))
                # Sleeping because sometimes getmany is just called in a loop
                # instead of ever releasing to the event loop
                await self.sleep(0)
                async for tp, message in ait:
                    offset = message.offset
                    r_offset = get_read_offset(tp)
                    if r_offset is None or offset > r_offset:
                        await callback(message)
                        set_read_offset(tp, offset)
                    else:
                        self.log.dev('DROPPED MESSAGE ROFF %r: k=%r v=%r',
                                     offset, message.key, message.value)
                unset_flag(flag_consumer_fetching)
        except self.consumer_stopped_errors:
            if self.transport.app.should_stop:
                # we're already stopping so ignore
                self.log.info('Broker stopped consumer, shutting down...')
                return
            raise
        except asyncio.CancelledError:
            if self.transport.app.should_stop:
                # we're already stopping so ignore
                self.log.info('Consumer shutting down for user cancel.')
                return
            raise
        except Exception as exc:
            self.log.exception('Drain messages raised: %r', exc)
            raise
        finally:
            unset_flag(flag_consumer_fetching)
            self.set_shutdown()
            fetcher.set_shutdown()

    @property
    def unacked(self) -> Set[Message]:
        return cast(Set[Message], self._unacked_messages)


class Fetcher(Service):
    app: AppT

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        super().__init__(**kwargs)

    @Service.task
    async def _fetcher(self) -> None:
        await cast(Consumer, self.app.consumer)._drain_messages(self)


class Producer(Service, ProducerT):
    """Base Producer."""

    def __init__(self, transport: TransportT, **kwargs: Any) -> None:
        self.transport = transport
        super().__init__(loop=self.transport.loop, **kwargs)

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int]) -> Awaitable[RecordMetadata]:
        raise NotImplementedError()

    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int]) -> RecordMetadata:
        raise NotImplementedError()

    def key_partition(self, topic: str, key: bytes) -> TP:
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

    def __init__(self,
                 url: Union[str, URL],
                 app: AppT,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.url = URL(url)
        self.app = app
        self.loop = loop

    def create_consumer(self, callback: ConsumerCallback,
                        **kwargs: Any) -> ConsumerT:
        return self.Consumer(self, callback=callback, **kwargs)

    def create_producer(self, **kwargs: Any) -> ProducerT:
        return self.Producer(self, **kwargs)
