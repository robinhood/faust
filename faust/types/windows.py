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


class WindowT(metaclass=abc.ABCMeta):
    expires: Optional[float]

    def windows(self, timestamp: float) -> List[WindowRange]:
        ...

    def stale_before(self) -> Optional[float]:
        ...
