import asyncio
from itertools import islice
from random import choice, random, shuffle
from typing import Any, Dict, Iterable, NamedTuple, Optional, Tuple
import pytest
from mode.utils.mocks import Mock
from faust.types import TP
from faust.transport.offsets import OffsetManager, Offsets

TP1 = TP('foo', 30)

BREAKPOINTS = {
    (2, 0): 'at beginning of batch',
    (8, 58): 'in middle of batch',
    (6, 99): 'at end of batch',
    (9, 1): 'at beginning of batch offset 1',
}

SLEEP_TIMES = [0.001, 0.0012, 0.0013, 0.0000]

# Test matrix parameters.

#: Offset starting point.
MATRIX_OFFSETS = [
    None,
    0,
    1000,
    33,
    1001,
    10001,
    999,
    99,
    1,
]

#: Offset breakpoint (rebalancing) enabled/not enabled.
MATRIX_BREAKPOINTS = [
    BREAKPOINTS,
    None,
]

#: Commit offset is the next offset to process?
#: In Kafka they usually commit offset + 1 when committing.
MATRIX_COMMIT_FUTURE = [False, True]

#: Randomize order in which offsets are acked (enabled/not enabled).
#: This is used to emulate how concurrent agents acknowledge events.
MATRIX_RANDOMIZE = [False, True]

#: Add random sleeping (enabled/not enabled).
MATRIX_TIMING = [False, True]

#: Add random committing during testing.
MATRIX_COMMITTING = [False, True]


class OffsetCase(NamedTuple):
    offset_start: int
    breakpoints: Optional[Dict[Tuple[int, int], str]]
    commit_future: bool
    randomize: bool
    timing: bool
    committing: bool


class OffsetInfo(NamedTuple):
    offset: int
    point: Tuple[int, int]
    batch: range


def _create_manager(*, offset_start: int, commit_future: bool,
                    commit_every: int = None) -> OffsetManager:
    consumer = Mock(name='consumer', _commit_every=None)
    consumer.app.conf.broker_commit_future = commit_future
    manager = OffsetManager(consumer)
    if commit_future:
        if offset_start is None:
            next_offset = 0
            prev_offset = None
        else:
            next_offset = offset_start
            prev_offset = offset_start - 1 if offset_start else None
    else:
        if offset_start is None:
            next_offset = 0
            prev_offset = None
        else:
            next_offset = offset_start + 1
            prev_offset = offset_start
    manager.on_seek_partitions({TP1: prev_offset})
    manager.on_post_commit({TP1: prev_offset})
    return manager, prev_offset, next_offset


@pytest.mark.parametrize('offset_start,commit_future', [
    (offset_start, commit_future)
    for offset_start in MATRIX_OFFSETS
    for commit_future in MATRIX_COMMIT_FUTURE
])
def test_OffsetQueue(offset_start, commit_future):
    manager, prev_offset, next_offset = _create_manager(
        offset_start=offset_start,
        commit_future=commit_future,
    )
    offsets = manager[TP1]
    key, first_bucket = offsets._bucket_for_offset(next_offset)
    assert first_bucket.first_offset == next_offset

    buckets = {}
    bucket_size = offsets.bucket_size
    for i in range(100):
        first_offset = bucket_size * i
        range_end = bucket_size * (i + 1)
        last_offset = range_end - 1
        print(i)
        for j in range(first_offset, range_end):
            if j < next_offset:
                # skip everything up to offset_start
                continue
            key, bucket = offsets._bucket_for_offset(j)
            if bucket is first_bucket:
                assert bucket.first_offset == next_offset
                real_first_offset = (
                    bucket.first_offset // bucket_size * bucket_size)
                already_acked = bucket.first_offset - real_first_offset
                assert bucket.total == bucket_size - already_acked
                assert list(bucket.point) == list(range(
                    next_offset, range_end))
                assert bucket.already_acked == already_acked
            else:
                assert bucket.first_offset == first_offset
                assert bucket.total == offsets.bucket_size
                assert list(bucket.point) == list(range(
                    first_offset, range_end))
                assert not bucket.already_acked
            assert bucket.last_offset == last_offset
            assert key == i
            assert not bucket.filled
            assert not bucket.offsets
            assert not bucket.full
            last_bucket = buckets.setdefault(i, bucket)
            if last_bucket is not None:
                assert bucket is last_bucket


def randomize_stream(it: Iterable[Any], *,
                     cluster_size: int = 8) -> Iterable[Any]:
    """Randomize values in a stream.

    We use this to simulate that acks do not always happen in order,
    so for every 8 numbers we receive we shuffle them to emulate
    how a concurrent agent would behave.

    (when testcase matrix randomize flag is enabled)
    """

    buffer = []
    for value in it:
        if len(buffer) >= cluster_size:
            shuffle(buffer)
            buffered_value = buffer.pop()
            yield buffered_value
        buffer.append(value)
    if buffer:
        shuffle(buffer)
        yield from iter(buffer)


def increasing_offsets(
        start: int, end: int, *,
        randomize: bool,
        batch_size: int = 100) -> Iterable[Tuple[range, int]]:
    start = start or 0
    did_offset = False
    for i, chunk in enumerate(chunks(iter(range(start, end)), batch_size)):
        batch = None
        first_offset = chunk[0]
        last_offset = chunk[-1] + 1
        if did_offset:
            batch = range(first_offset, last_offset)
            print(f'>>> BATCH: {batch!r}')

        offsets_stream = range(first_offset, last_offset)
        if randomize:
            offsets_stream = randomize_stream(offsets_stream)
        for j in offsets_stream:
            if not did_offset and j >= start:
                did_offset = True
                batch = range(start, last_offset)
                print(f'>>> BATCH: {batch!r}')
            if batch is not None:
                yield OffsetInfo(offset=j, point=(i, j), batch=batch)


def acking_offsets(start: int, *, end: int = None,
                   manager: OffsetManager,
                   breakpoints: Dict[Tuple[int, int], str],
                   randomize: bool = False,
                   batch_size: int = 100) -> Iterable[Tuple[range, int]]:
    if end is None:
        end = (start or 0) + 1000
    offsets = increasing_offsets(start, end,
                                 batch_size=batch_size,
                                 randomize=randomize)

    while True:
        (offset, point, batch) = next(offsets, (None, None, None))
        if offset is None:
            break
        if breakpoints and point in breakpoints:
            offsets = increasing_offsets(offset + 1, end,
                                         randomize=randomize,
                                         batch_size=batch_size)
            new_offsets = manager.flush_deep({TP1})
            manager.on_post_commit(new_offsets)
            manager.on_seek_partitions({
                tp: offset + 1
                for tp, offset in new_offsets.items()
            })
        else:
            yield (batch, offset)


@pytest.mark.parametrize(
    'offset_start,breakpoints,commit_future,randomize,timing,committing',
    [
        (offset, breakpoint, commit_future, randomize, timing, committing)
        for offset in MATRIX_OFFSETS
        for breakpoint in MATRIX_BREAKPOINTS
        for commit_future in MATRIX_COMMIT_FUTURE
        for randomize in MATRIX_RANDOMIZE
        for timing in MATRIX_TIMING
        for committing in MATRIX_COMMITTING
    ],
)
@pytest.mark.asyncio
async def test_offsets(
        offset_start,
        breakpoints,
        commit_future,
        randomize,
        timing,
        committing):
    manager, prev_offset, next_offset = _create_manager(
        offset_start=offset_start,
        commit_future=commit_future,
        commit_every=500,
    )
    flush_count = 0
    expected_flush_count = 10
    for batch, offset in acking_offsets(next_offset,
                                        manager=manager,
                                        breakpoints=breakpoints,
                                        randomize=randomize):
        if timing:
            sleeptime = choice([None] * 20 + SLEEP_TIMES)
            if sleeptime is not None:
                await asyncio.sleep(sleeptime)

        did_flush = False
        if manager[TP1].ack(offset):
            did_flush = True
            _, bucket = manager[TP1]._bucket_for_offset(offset)
            expected_offset = bucket.last_offset
            flush_count += 1
            assert manager.get_pending_offset(TP1) == expected_offset
            manager.on_post_commit(manager._pending_offset)
        if committing and random() > 0.95:
            manager.on_post_commit(manager._pending_offset)

        if offset == batch.stop:
            assert did_flush

    assert flush_count == expected_flush_count


@pytest.mark.parametrize('commit_future', MATRIX_COMMIT_FUTURE)
def test_simple(commit_future):
    manager, prev_offset, next_offset = _create_manager(
        offset_start=None,
        commit_future=commit_future,
    )
    offsets = Offsets(
        TP1, None,
        buckets_prefill=1,
        manager=manager,
    )
    assert len(offsets.buckets) == 1

    assert manager.get_committed_offset(TP1) is None

    current_offset = -1

    def fill(start, stop):
        for j, i in enumerate(range(start, stop)):
            offsets.ack(i)
            assert len(offsets.buckets) == 1
            assert not offsets.filled

            key = i // 100

            assert offsets.buckets[key].total == 100
            assert offsets.buckets[key].filled == j + 1
            assert sorted(offsets.buckets[key].offsets) == list(
                range(start, i + 1))
            if not current_offset or current_offset < 0:
                expected_offset = None
            else:
                expected_offset = current_offset
            assert manager.get_pending_offset(TP1) == expected_offset

    def flush(last_offset):
        nonlocal current_offset
        offsets.ack(last_offset)

        current_offset = manager.get_pending_offset(TP1)
        assert current_offset == last_offset

    def flush_accurate():
        nonlocal current_offset
        current_offset = offsets.flush_accurate(current_offset)

    print('FILL 0')
    fill(0, 99)
    print('FLUSH')
    flush(99)
    assert 0 not in offsets.buckets
    print('FILL 100')
    fill(100, 199)
    flush(199)
    assert 1 not in offsets.buckets
    fill(200, 299)
    flush(299)
    assert 2 not in offsets.buckets

    assert not offsets.buckets
    assert not offsets.filled

    fill(300, 315)
    offsets.ack(316)
    offsets.ack(317)
    offsets.ack(318)
    offsets.ack(400)
    flush_accurate()
    assert current_offset == 314


def chunks(it, n):
    for item in it:
        yield [item] + list(islice(it, n - 1))
