"""Windowing strategies."""
from datetime import datetime
from typing import Optional, List
from .types import WindowRange, WindowT


class HoppingWindow(WindowT):
    """Fixed-size, overlapping windows
    """
    size: float
    step: float

    def __init__(self, size: float, step: float,
                 expires: Optional[float]) -> None:
        self.size = size
        self.step = step
        self.expires = expires

    def windows(self, timestamp: float) -> List[WindowRange]:
        curr = self._timestamp_window(timestamp)
        earliest = curr.start - self.size + self.step
        return[
            WindowRange.from_start(start, self.size)
            for start in range(earliest, curr.end, self.step)
        ]

    def _timestamp_window(self, timestamp: float) -> WindowRange:
        start = (timestamp // self.step) * self.step
        return WindowRange.from_start(start, self.size)

    def stale_before(self) -> Optional[float]:
        return self._stale_before(self.expires) \
            if self.expires is not None else None

    def _stale_before(self, expires: float) -> float:
        now = datetime.utcnow().timestamp()
        return self._timestamp_window(now - expires).start


class TumblingWindow(HoppingWindow):
    """Fixed-size, non-overlapping, gap-less windows
    """

    def __init__(self, size: float, expires: Optional[float]) -> None:
        super(TumblingWindow, self).__init__(size, size, expires)


class SlidingWindow(WindowT):
    """Fixed-size, overlapping windows that work on
       differences between record timestamps
    """
    before: float
    after: float

    def __init__(self, before: float, after: float, expires: float) -> None:
        self.before = before
        self.after = after
        self.expires = expires

    def windows(self, timestamp: float) -> List[WindowRange]:
        """
        SELECT * FROM stream1, stream2
        WHERE
         stream1.key = stream2.key
        AND
         stream1.ts - before <= stream2.ts AND stream2.ts <= stream1.ts + after
        """
        return [WindowRange(start=timestamp - self.before,
                            end=timestamp + self.after)]

    def stale_before(self) -> Optional[float]:
        return self._stale_before(self.expires) \
            if self.expires is not None else None

    @classmethod
    def _stale_before(cls, expires: float) -> float:
        return datetime.utcnow().timestamp() - expires
