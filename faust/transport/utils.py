"""Transport utils - scheduling."""
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
)
from mode.utils.compat import OrderedDict
from faust.types import TP
from faust.types.transports import SchedulingStrategyT

__all__ = [
    'TopicIndexMap',
    'DefaultSchedulingStrategy',
    'TopicBuffer',
]

# But we want to process records from topics in round-robin order.
# We convert records into a mapping from topic-name to "chain-of-buffers":
#   topic_index['topic-name'] = chain(all_topic_partition_buffers)
# This means we can get the next message available in any topic
# by doing: next(topic_index['topic_name'])
TopicIndexMap = MutableMapping[str, 'TopicBuffer']


class DefaultSchedulingStrategy(SchedulingStrategyT):
    """Consumer record scheduler.

    Delivers records in round robin between both topics and partitions.
    """

    @classmethod
    def map_from_records(cls, records: Mapping[TP, List]) -> TopicIndexMap:
        topic_index: TopicIndexMap = {}
        for tp, messages in records.items():
            try:
                entry = topic_index[tp.topic]
            except KeyError:
                entry = topic_index[tp.topic] = TopicBuffer()
            entry.add(tp, messages)
        return topic_index

    def iterate(self, records: Mapping[TP, List]) -> Iterator[Tuple[TP, Any]]:
        return self.records_iterator(self.map_from_records(records))

    def records_iterator(self,
                         index: TopicIndexMap) -> Iterator[Tuple[TP, Any]]:
        to_remove: Set[str] = set()
        sentinel = object()
        _next = next
        while index:
            for topic in to_remove:
                index.pop(topic, None)
            for topic, messages in index.items():
                item = _next(messages, sentinel)
                if item is sentinel:
                    # this topic is now empty,
                    # but we cannot remove from dict while iterating over it,
                    # so move that to the outer loop.
                    to_remove.add(topic)
                    continue
                tp, record = item  # type: ignore
                yield tp, record


class TopicBuffer(Iterator):
    """Data structure managing the buffer for incoming records in a topic."""

    _buffers: Dict[TP, Iterator]
    _it: Optional[Iterator]

    def __init__(self) -> None:
        # note: this is a regular dict, but ordered on Python 3.6
        # we use this alias to signify it must be ordered.
        self._buffers = OrderedDict()
        # getmany calls next(_TopicBuffer), and does not call iter(),
        # so the first call to next caches an iterator.
        self._it = None

    def add(self, tp: TP, buffer: List) -> None:
        assert tp not in self._buffers
        self._buffers[tp] = iter(buffer)

    def __iter__(self) -> Iterator[Tuple[TP, Any]]:
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

    def __next__(self) -> Tuple[TP, Any]:
        # Note: this method is not in normal iteration
        # as __iter__ returns generator.
        it = self._it
        if it is None:
            it = self._it = iter(self)
        return it.__next__()
