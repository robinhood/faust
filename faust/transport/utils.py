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

# But we want to process records from topics in round-robin order.
# We convert records into a mapping from topic-name to "chain-of-buffers":
#   topic_index['topic-name'] = chain(all_topic_partition_buffers)
# This means we can get the next message available in any topic
# by doing: next(topic_index['topic_name'])
TopicIndexMap = MutableMapping[str, 'TopicBuffer']


class TopicBuffer(Iterator):
    _buffers: Dict[TP, Iterator]
    _it: Optional[Iterator]

    @classmethod
    def map_from_records(cls, records: Mapping[TP, List]) -> TopicIndexMap:
        topic_index: TopicIndexMap = {}
        for tp, messages in records.items():
            try:
                entry = topic_index[tp.topic]
            except KeyError:
                entry = topic_index[tp.topic] = cls()
            entry.add(tp, messages)
        return topic_index

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
