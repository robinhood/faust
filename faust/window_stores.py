from collections import defaultdict
from typing import Tuple, MutableMapping, List
from .types import WindowRange, WindowT, WindowStoreT, K, WindowedEvent, Event


class AggregationWindowStore(WindowStoreT):
    """Events could fall into 1 or many discrete time windows
    """
    _store: MutableMapping[Tuple[K, WindowRange], WindowedEvent]

    def __init__(self, window_strategy: WindowT) -> None:
        self._store = {}
        self.window_strategy = window_strategy

    def get(self, key: K, timestamp: float) -> List[WindowedEvent]:
        windows = self.window_strategy.windows(timestamp)
        events = []
        for window in windows:
            store_key = (key, window)
            if store_key in self._store:
                events.append(self._store[store_key])
        return events

    def put(self, key: K, windowed_event: WindowedEvent) -> None:
        windowed_event = W
        self._store[(key, windowed_event.window_range)] = windowed_event


class SlidingJoinWindowStore(WindowStoreT):
    _store: MutableMapping[K, List[Event]]

    def __init__(self, window_strategy) -> None:
        self._store = defaultdict(List)
        self.window_strategy = window_strategy

    def get(self, key: K, timestamp: float) -> List[WindowedEvent]:
        window = self.window_strategy.windows(timestamp)[0]
        events = self._store[key]
        return [event for event in events
                if window.start <= event.req.message.timestamp < window.end]

    def put(self, key: K, windowed_event: WindowedEvent):
        self._store[key].append(windowed_event.event)
