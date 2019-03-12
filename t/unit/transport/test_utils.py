from faust.types import TP
from faust.transport.utils import (
    DefaultSchedulingStrategy,
    TopicBuffer,
)

TP1 = TP('foo', 0)
TP2 = TP('foo', 1)
TP3 = TP('bar', 0)
TP4 = TP('bar', 1)
TP5 = TP('baz', 3)


BUF1 = [0, 1, 2, 3, 4]
BUF2 = [5, 6, 7, 8]
BUF3 = [9, 10]
BUF4 = [11, 12, 13]
BUF5 = [14, 15]


class test_TopicBuffer:

    def test_iter(self):
        buffer = TopicBuffer()
        buffer.add(TP1, BUF1)
        buffer.add(TP2, BUF2)
        buffer.add(TP3, BUF3)
        buffer.add(TP4, BUF4)
        buffer.add(TP5, BUF5)

        consumed = []
        for tp, item in buffer:
            consumed.append((tp, item))

        assert consumed == [
            (TP1, 0),
            (TP2, 5),
            (TP3, 9),
            (TP4, 11),
            (TP5, 14),

            (TP1, 1),
            (TP2, 6),
            (TP3, 10),
            (TP4, 12),
            (TP5, 15),

            (TP1, 2),
            (TP2, 7),
            (TP4, 13),

            (TP1, 3),
            (TP2, 8),

            (TP1, 4),
        ]

    def test_map_from_records(self):
        records = {TP1: BUF1, TP2: BUF2, TP3: BUF3}
        m = DefaultSchedulingStrategy.map_from_records(records)
        assert isinstance(m['foo'], TopicBuffer)
        buf1 = m['foo']._buffers
        assert len(buf1) == 2
        assert list(buf1[TP1]) == BUF1
        assert list(buf1[TP2]) == BUF2

        assert isinstance(m['bar'], TopicBuffer)
        buf2 = m['bar']._buffers
        assert list(buf2[TP3]) == BUF3

    def test_next(self):
        buffer = TopicBuffer()
        buffer.add(TP1, BUF1)

        consumed = []
        while True:
            try:
                consumed.append(next(buffer))
            except StopIteration:
                break
        assert consumed == [
            (TP1, 0),
            (TP1, 1),
            (TP1, 2),
            (TP1, 3),
            (TP1, 4),
        ]
