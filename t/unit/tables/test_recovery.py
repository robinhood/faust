import pytest
from faust.tables.recovery import Recovery
from faust.types import TP

TP1 = TP('foo', 0)
TP2 = TP('foo', 1)


@pytest.fixture()
def recovery(*, app):
    return Recovery(app, app.tables)


@pytest.mark.parametrize('highwaters,offsets,needs_recovery,total,remaining', [
    ({TP1: 0, TP2: -1}, {TP1: -1, TP2: -1}, True, 1, {TP1: 1, TP2: 0}),
    ({TP1: -1, TP2: -1}, {TP1: -1, TP2: -1}, False, 0, {TP1: 0, TP2: 0}),
    ({TP1: 100, TP2: -1}, {TP1: -1, TP2: -1}, True, 101, {TP1: 101, TP2: 0}),
])
def test_recovery_from_offset_0(
        highwaters, offsets, needs_recovery, total, remaining, *,
        recovery):
    recovery.active_highwaters.update(highwaters)
    recovery.active_offsets.update(offsets)

    if needs_recovery:
        assert recovery.need_recovery()
    else:
        assert not recovery.need_recovery()
    assert recovery.active_remaining_total() == total
    if remaining:
        assert recovery.active_remaining() == remaining
