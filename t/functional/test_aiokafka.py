from faust.types import TP
from aiokafka.structs import TopicPartition


def test_TP_TopicPartition_hashability():
    d = {}
    d[TP('foo', 33)] = 33
    assert d[TopicPartition('foo', 33)] == 33
