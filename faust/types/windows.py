import abc
from typing import Optional, List, NamedTuple
from .core import K
from .models import Event


class WindowRange(NamedTuple):
    start: Optional[float]
    end: Optional[float]

    @classmethod
    def from_start(cls, start: float, size: float) -> 'WindowRange':
        return cls(start=start, end=start + size)


class WindowedEvent(NamedTuple):
    window_range: WindowRange
    event: Event


class WindowT(metaclass=abc.ABCMeta):
    expires: Optional[float]

    def windows(self, timestamp: float) -> List[WindowRange]:
        ...

    def stale_before(self) -> Optional[float]:
        ...


class WindowStoreT(metaclass=abc.ABCMeta):
    window_strategy: WindowT

    def get(self, key: K, timestamp: float) -> List[WindowedEvent]:
        """Get windowed events for this key for the given timestamp.
           In the case of aggregates, this returns all key-value pairs for
           windows with this timerange.

           From stream-stream windowed joins, this could be used to get all
           key-value pairs for this timestamp that the key should be
           joined with
        """
        ...

    def put(self, key: K, event: Event) -> None:
        ...
