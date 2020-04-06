import asyncio
import os
from collections import defaultdict
from enum import Enum
from itertools import count
from typing import (
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
)
from mode.utils.collections import FastUserDict, Heap
from mode.utils.futures import notify
from mode.utils.logging import get_logger
from mode.utils.objects import cached_property
from faust.types import ConsumerT, TP
from faust.utils.functional import consecutive_numbers
from .utils import ensure_TP

DEBUG = bool(os.environ.get('F_OFFSETCHECK', False))

logger = get_logger(__name__)


class FlushStrategy(Enum):
    # Accurate means it will always do an iterative flush
    # to find the currently commitable offset (slow for many partitions)
    ACCURATE = 'ACCURATE'
    # Buffering acks by up to 100 at a time, makes it completely
    # free to find the current committable offset.
    FAST = 'FAST'


class OffsetQueue:
    """Represents an offset range waiting to be acknowledged."""

    #: Range of offsets in this queue.
    point: range

    #: Total number of offsets expected in this queue
    #: before the offsets can be committed.
    total: int

    #: Set of offsets acknowledged so far.
    offsets: Set[int]

    #: Set of offsets that have been marked as gaps.
    gaps: Set[int]

    #: Flag set when offsets can be committed
    #: (all offsets in this range have been acknowledged).
    full: bool

    #: Current number of offsets filled.
    filled: int

    def __init__(self, start: int, stop: int,
                 already_acked: int = 0) -> None:
        self.first_offset = start + already_acked
        self.last_offset = stop - 1
        assert self.last_offset >= self.first_offset
        self.point = range(self.first_offset, stop)
        assert self.point.start >= 0, self.point.start
        assert self.point.stop >= 0, self.point.stop
        self.total = len(self.point)
        self.offsets = set()
        self.full = False
        self.filled = 0
        self.already_acked = already_acked

    def ack(self, offset: int) -> bool:
        total = self.total
        offsets = self.offsets
        if offset not in offsets:
            offsets.add(offset)
            self.filled = filled = self.filled + 1
            assert not filled > total
            if filled == total:
                self.full = True
                return True
        return False

    def add_gap(self, offset: int) -> None:
        self.offsets.add(offset)
        self.gaps.add(offset)
        self.filled += 1

    def missing(self) -> Set[int]:
        return self._all_offsets - self.offsets

    @cached_property
    def _all_offsets(self) -> Set[int]:
        return set(self.point)

    @property
    def commit_offset(self) -> int:
        return self.point.stop - 1

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self.filled}/{self.total}>'


class _FilledHeapEntry(NamedTuple):
    key: int
    offset: int
    batch_first_offset: int
    batch_last_offset: int


class Offsets:

    _OffsetQueue: ClassVar[Type[OffsetQueue]] = OffsetQueue

    strategy: FlushStrategy = FlushStrategy.ACCURATE
    strategy_handler: Dict[FlushStrategy, Callable[[int], int]]

    tp: TP
    bucket_size: int = 100
    buckets: MutableMapping[int, OffsetQueue]
    filled: Heap[_FilledHeapEntry]

    # We keep track of filled queues (those that can be committed)
    # in the Offsets.filled deque, and we want to verify that
    # we do not store duplicates in that list.
    # Doing so would be O(n) at worst, so we want to avoid doing this.
    # Luckily we know that the offsets that can be committed are always
    # increasing so we can just keep the last filled bucket.
    last_filled_bucket: Optional[int]

    buckets_prefill: int = 100  # 10k offsets

    #: The currently committable offset.
    #: This is updated as buckets are filled.
    committable_offset: Optional[int] = None

    def __init__(self, tp: TP, offset_start: int, *,
                 manager: 'Offsets',
                 buckets_prefill: int = 1) -> None:
        self.tp = tp
        self.buckets = {}
        self.filled = Heap()
        self.offset_start = offset_start
        self.manager = manager
        assert buckets_prefill  # need to prefill first bucket at least
        if buckets_prefill is not None:
            self.buckets_prefill = buckets_prefill
        self._prefill_buckets(self.offset_start)
        self.strategy_handler = {
            FlushStrategy.FAST: self.flush_fast,
            FlushStrategy.ACCURATE: self.flush_accurate,
        }

    def partial_buckets(self) -> Mapping[int, List[int]]:
        return {
            key: sorted(bucket.missing())
            for key, bucket in self.buckets.items()
            if bucket.filled
        }

    def flush_verify(self, last_offset: int, *,
                     accurate: bool = False) -> int:
        # we could have earlier buckets since keys are not inserted
        # in order, but at least we will have a number high enough
        # to stop the loop rather quickly.
        first_bucket_key = next(iter(self.buckets.keys()), 0)
        if last_offset is None:
            last_offset = -1
        last_bucket = last_offset // self.bucket_size
        next_committable: Optional[int] = None
        for i in count(last_bucket):
            try:
                bucket = self.buckets[i]
            except KeyError:
                if next_committable is not None or i > first_bucket_key:
                    break
            else:
                include_bucket = False
                if accurate:
                    include_bucket = bucket.offsets  # has any offsets at all
                else:
                    include_bucket = bucket.filled   # buffered
                if include_bucket:
                    offsets = sorted(bucket.offsets)
                    next_committable = next(consecutive_numbers(offsets))[-1]
                else:
                    break
        if next_committable is None:
            return last_offset
        return next_committable

    def flush(self, last_offset: int, *, debug: bool = DEBUG) -> int:
        return self.flush_fast(last_offset, debug=debug)

    def _maybe_transition_strategy(self,
                                   prev_offset: int,
                                   current_offset: int) -> None:
        strategy = self.strategy
        diff = current_offset - prev_offset
        assert diff >= 0
        if not diff:
            return
        if strategy == FlushStrategy.ACCURATE and diff > 100:
            logger.info(
                'Optimizing commit strategy for tp %s to FAST (diff=%r)',
                self.tp, diff)
            self.strategy = FlushStrategy.FAST
        elif strategy == FlushStrategy.FAST and diff <= 10:
            logger.info(
                'Optimizing commit strategy for tp %s to ACCURATE (diff=%r)',
                self.tp, diff)
            self.strategy = FlushStrategy.ACCURATE

    def flush_fast(self, last_offset: int, *,
                   debug: bool = DEBUG) -> Optional[int]:
        if debug:
            expected = self.flush_verify(last_offset, accurate=False)
            new_offset = self._flush_fast(last_offset)
            assert new_offset == expected, (new_offset, expected)
            return new_offset
        else:
            return self._flush_fast(last_offset)

    def _flush_fast(self, last_offset: int) -> int:
        offset: Optional[int] = None
        filled = self.filled

        # spool over batches that are < last committed offset.
        if last_offset is None:
            expected_next_offset = 0
        else:
            expected_next_offset = last_offset
        while filled and filled[0].batch_last_offset < expected_next_offset:
            entry = filled.pop()
        while 1:
            if filled:
                if last_offset is None:
                    expected_next_offset = 0
                else:
                    expected_next_offset = last_offset + 1
                if filled[0].batch_first_offset == expected_next_offset:
                    entry = filled.pop()
                    offset = entry.offset
                    self.manager.on_new_offset(self.tp, offset)
                    last_offset = entry.batch_last_offset
                else:
                    break
            else:
                break

        if offset is None:
            return last_offset
        else:
            return offset

    def flush_accurate(self, last_offset: int, *,
                       debug: bool = DEBUG) -> Optional[int]:
        offset = self.flush_fast(last_offset, debug=debug)
        if debug:
            expected_offset = self.flush_verify(offset, accurate=True)
            new_offset = self._flush_accurate(offset)
            assert new_offset == expected_offset, (new_offset, expected_offset)
            return new_offset
        return self._flush_accurate(offset)

    def _flush_accurate(self, last_offset: int) -> Optional[int]:
        buckets = sorted(self.buckets.items())
        # this keeps order, as dictionaries are ordered.
        filled_buckets = {}
        for key, bucket in buckets:
            if bucket.filled:
                filled_buckets[key] = bucket
            else:
                break

        offset = last_offset
        for _, bucket in sorted(filled_buckets.items()):
            if offset is None or offset <= 0:
                expected_next_offset = 0
            else:
                expected_next_offset = offset + 1
            if bucket.first_offset == expected_next_offset:
                offsets = sorted(bucket.offsets)
                committable_range = next(consecutive_numbers(offsets))
                offset = committable_range[-1]
                if not bucket.full:
                    return offset
        return offset

    def add_gap(self, offset_start: int, offset_end: int) -> None:
        committed_offset = self.manager.get_committed_offset(self.tp)
        for offset in range(offset_start, offset_end):
            if offset > committed_offset:
                key, bucket = self._bucket_for_offset(offset)
                bucket.add_gap(offset)

    def _prefill_buckets(self, offset_start: int) -> None:
        prefill = self.buckets_prefill
        first_key = (offset_start or 0) // self.bucket_size
        first_offset = first_key * self.bucket_size
        if offset_start is None:
            already_acked = 0
        else:
            already_acked = offset_start - first_offset
        self.buckets[first_key] = self._new_bucket_from_offset(
            first_offset, already_acked=already_acked)
        bucket_keys = list(range(first_key, first_key + prefill))
        self.buckets.update({
            i: self._new_bucket_from_key(i)
            for i in bucket_keys[1:]
        })

    def _new_bucket_from_offset(self, offset_start: int, *,
                                already_acked: int = 0) -> OffsetQueue:
        return self._OffsetQueue(
            offset_start, offset_start + self.bucket_size,
            already_acked=already_acked,
        )

    def _new_bucket_from_key(self, key: int) -> OffsetQueue:
        return self._new_bucket_from_offset(key * self.bucket_size)

    def ack(self, offset: int) -> bool:
        assert offset >= 0, offset
        key, bucket = self._bucket_for_offset(offset)
        if bucket.ack(offset):
            self.on_bucket_filled(key, bucket)
            return True
        return False

    def on_bucket_filled(self, key: int, bucket: OffsetQueue) -> None:
        popped_bucket = self.buckets.pop(key, None)
        assert popped_bucket is bucket
        interval_start = key * self.bucket_size
        expected_starting_point = interval_start + bucket.already_acked
        assert bucket.point.start == expected_starting_point
        self.filled.push(_FilledHeapEntry(
            key,
            bucket.commit_offset,
            bucket.first_offset,
            bucket.last_offset,
        ))
        current_offset = self.manager.get_pending_offset(self.tp)
        self._flush_fast(current_offset)

    def _bucket_for_offset(self, offset: int) -> Tuple[int, OffsetQueue]:
        key = offset // self.bucket_size
        bucket = self.buckets.get(key)
        if bucket is None:
            bucket = self.buckets[key] = self._new_bucket_from_key(key)
        return key, bucket


class OffsetManager(FastUserDict[TP, Offsets]):

    _Offsets: ClassVar[Type[Offsets]] = Offsets

    #: Total number of calls to :meth:`ack`.
    acked_total: int

    #: Keeps track of the currently read offset in each TP
    #: This is the highest offset of any record that we have read so far.
    #: If we see the message again we will discard it (logs a message if
    #: :envvar:`DEVLOG` is enabled).
    read_offset: MutableMapping[TP, Optional[int]]

    #: Keeps track of the currently uncommitted offset.
    #: Once Consumer.commit() is called this offset will be flushed
    #: to Kafka and :attr:`_committed_offset` will be updated with the new
    #: offsets.
    #: This data structure is updated:
    #:   - when a message is acked and a new offset can be set
    #:     because we have a consecutive range of offsets acked falling
    #:     after the currently committed offset.
    #:     For example if the last committed offset is 10, and we have acked
    #:     offsets [11, 12, 13, 15] we can commit new offset 13.
    _pending_offset: MutableMapping[TP, Optional[int]]

    #: The currently committed offset in Kafka.
    #: This data structure is updated:
    #:    - At worker start (:meth:`on_seek_partitions`)
    #:    - After rebalance (:meth:`on_seek_partitions`)
    #:    - After consumer commit (:meth:`on_post_commit`).
    _committed_offset: MutableMapping[TP, Optional[int]]

    _gap_offset: MutableMapping[TP, Set[Tuple[int, int]]]

    #: The consumer.wait_empty() method will set this to be notified
    #: when something acks a message.
    _waiting_for_ack: Optional[asyncio.Future] = None

    def __init__(self, consumer: ConsumerT) -> None:
        self.consumer = consumer
        self.data = {}
        self.acked_total = 0
        self.read_offset = defaultdict(lambda: None)
        self._pending_offset = {}
        self._committed_offset = {}
        self._gap_offset = defaultdict(set)
        self._waiting_for_ack = None

    def get_pending_offset(self, tp: TP) -> Optional[int]:
        return self._pending_offset.get(tp)

    def get_committed_offset(self, tp: TP) -> Optional[int]:
        return self._committed_offset.get(tp)

    def add_gap(self, tp: TP, offset_from: int, offset_to: int) -> None:
        self._gap_offset[tp].add((offset_from, offset_to))
        self[tp].add_gap(offset_from, offset_to)

    def _reapply_gaps(self) -> None:
        for tp, gaps in self._gap_offset.items():
            for (offset_from, offset_to) in sorted(gaps):
                self.add_gap(tp, offset_from, offset_to)

    def on_new_offset(self, tp: TP, offset: int) -> None:
        # updates the "committable offsets" index with a new offset
        # commit() will then use this to commit the actual offset in the
        # broker.
        commit_every = self.consumer._commit_every
        last_offset = self._committed_offset.get(tp)
        self._pending_offset[tp] = offset
        if commit_every is not None and last_offset:
            diff = offset - last_offset
            if diff >= commit_every:
                self.consumer.commit_soon()

    def ack(self, tp: TP, offset: int) -> bool:
        tp = ensure_TP(tp)
        try:
            self.acked_total += 1
            self[tp].ack(offset)
            return True
        finally:
            notify(self._waiting_for_ack)

    def flush(self, tps: Iterable[TP]) -> Optional[Dict[TP, int]]:
        return self._pending_offset

    def flush_deep(self, tps: Iterable[TP]) -> Optional[Dict[TP, int]]:
        get_pending = self._pending_offset.get
        committable = {
            tp: self[tp].flush_accurate(get_pending(tp))
            for tp in tps
        }
        return committable

    def on_post_commit(self, committed_offsets: Mapping[TP, int]) -> None:
        if DEBUG:
            get_committed = self._committed_offset.get
            for tp, offset in committed_offsets.items():
                last_committed = get_committed(tp)
                if last_committed is not None:
                    assert offset >= last_committed
        self._pending_offset.update(committed_offsets)
        self._committed_offset.update(committed_offsets)

    async def wait_for_ack(self, timeout: float) -> None:
        # arm future so that `ack()` can wake us up
        self._waiting_for_ack = asyncio.Future()
        try:
            # wait for `ack()` to wake us up
            await asyncio.wait_for(self._waiting_for_ack, timeout=1.0)
        except (asyncio.TimeoutError,
                asyncio.CancelledError):  # pragma: no cover
            pass
        finally:
            self._waiting_for_ack = None

    def on_seek_partitions(self, committed_offsets: Mapping[TP, int]) -> None:
        # offset + 1 is the next message to be processed.
        self.read_offset.update(committed_offsets)
        self._pending_offset.update(committed_offsets)
        self._committed_offset.update(committed_offsets)
        self.update_from_offsets(committed_offsets)

    def on_seek_partition(self, tp: TP, offset: int) -> None:
        # offset + 1 is the next message to be processed.

        # set new read offset will allow processing of any offset
        # greater than offset.
        tp = ensure_TP(tp)
        self.read_offset[tp] = offset
        # XXX should we update commit offset here?
        self._pending_offset[tp] = offset
        self._committed_offset[tp] = offset
        starting_offset = 0 if offset is None else offset + 1
        self.data[tp] = self._new_offsets(tp, starting_offset=starting_offset)

    def update_from_offsets(self,
                            offsets: Mapping[TP, Optional[int]]) -> None:
        # NOTE: We only update from offsets during rebalancing, and
        # that's after the last offsets have been committed already
        # (safe to overwrite)
        self.data.update({
            tp: self._new_offsets(
                tp,
                starting_offset=0 if offset is None else offset + 1,
            )
            for tp, offset in offsets.items()
        })
        self._reapply_gaps()

    def _new_offsets(self, tp: TP, *, starting_offset: int) -> Offsets:
        return self._Offsets(
            tp,
            manager=self,
            offset_start=starting_offset,
        )
