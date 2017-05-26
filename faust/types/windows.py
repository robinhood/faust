import abc
from typing import Optional, List, NamedTuple
from ..utils.times import Seconds


class WindowRange(NamedTuple):
    start: float = None
    end: float = None


class WindowT(metaclass=abc.ABCMeta):
    expires: float = None

    @abc.abstractmethod
    def ranges(self, timestamp: float) -> List[WindowRange]:
        ...

    @abc.abstractmethod
    def stale(self, timestamp: float) -> bool:
        ...



    @abc.abstractmethod
    def current(self, timestamp: float) -> WindowRange:
        ...

    @abc.abstractmethod
    def delta(self, timestamp: float, d: Seconds) -> WindowRange:
        ...
