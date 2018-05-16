import abc
from datetime import timezone
from typing import List, NamedTuple, Optional

from mode import Seconds

__all__ = ['WindowRange', 'WindowT']


class WindowRange(NamedTuple):
    start: float
    end: float

    @classmethod
    def from_start(cls, start: float, size: float) -> 'WindowRange':
        return cls(start=start, end=start + size)


class WindowT(abc.ABC):
    expires: Optional[float] = None
    tz: Optional[timezone] = None

    @abc.abstractmethod
    def ranges(self, timestamp: float) -> List[WindowRange]:
        ...

    @abc.abstractmethod
    def stale(self, timestamp: float, latest_timestamp: float) -> bool:
        ...

    @abc.abstractmethod
    def current(self, timestamp: float) -> WindowRange:
        ...

    @abc.abstractmethod
    def delta(self, timestamp: float, d: Seconds) -> WindowRange:
        ...
