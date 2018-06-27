from typing import Callable, Iterator, List
from faust.utils import venusian
from faust.types import RecordMetadata

SCAN_PRODUCER = 'faust-producer'

ProducerFun = Callable[[int], Iterator[RecordMetadata]]

producers: List[ProducerFun] = []


def producer(fun: ProducerFun) -> ProducerFun:
    venusian.attach(fun, category=SCAN_PRODUCER)
    producers.append(fun)
    return fun
