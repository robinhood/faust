import abc
from datetime import timezone
from typing import List, NamedTuple

from mode import Seconds


class WindowRange(NamedTuple):
    start: float = None
    end: float = None

    @classmethod
    def from_start(cls, start: float, size: float) -> 'WindowRange':
        return cls(start=start, end=start + size)


class WindowT(abc.ABC):
    expires: float = None
    tz: timezone = None

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
