"""Types related to windowing."""
import abc
from datetime import timezone
from typing import List, Optional, Tuple

from mode import Seconds

__all__ = ['WindowRange', 'WindowT']


WindowRange = Tuple[float, float]


def WindowRange_from_start(start: float, size: float) -> WindowRange:
    """Create new windowrange from start and size."""
    end = start + size - 0.1
    return (start, end)


class WindowT(abc.ABC):
    """Type class for windows."""

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
    def earliest(self, timestamp: float) -> WindowRange:
        ...

    @abc.abstractmethod
    def delta(self, timestamp: float, d: Seconds) -> WindowRange:
        ...
