"""Windowing strategies."""
from datetime import datetime
from typing import Optional, List
from .types import WindowRange, WindowT
from .utils.times import Seconds, want_seconds

__all__ = ['HoppingWindow', 'TumblingWindow', 'SlidingWindow']


# XXX mypy doesn't allow methods in NamedTuples
# but seems like a bug.
# This should be changed to WindowRange.from_start once that's fixed.
def _range_from_start(start: float, size: float) -> WindowRange:
    return WindowRange(start=start, end=start + size)


class HoppingWindow(WindowT):
    """Hopping window type.

    Fixed-size, overlapping windows.
    """
    size: float
    step: float

    def __init__(self, size: Seconds, step: Seconds,
                 expires: Seconds = None) -> None:
        self.size = want_seconds(size)
        self.step = want_seconds(step)
        self.expires = want_seconds(expires)

    def windows(self, timestamp: float) -> List[WindowRange]:
        curr = self._timestamp_window(timestamp)
        earliest = curr.start - self.size + self.step
        return [
            _range_from_start(start, self.size)
            for start in range(int(earliest), int(curr.end), int(self.step))
        ]

    def stale_before(self) -> Optional[float]:
        return (
            self._stale_before(self.expires)
            if self.expires else None
        )

    def current(self, timestamp: float) -> WindowRange:
        return self._timestamp_window(timestamp)

    def delta(self, timestamp: float, d: Seconds) -> WindowRange:
        return self._timestamp_window(timestamp - want_seconds(d))

    def _timestamp_window(self, timestamp: float) -> WindowRange:
        start = (timestamp // self.step) * self.step
        return _range_from_start(start, self.size)

    def _stale_before(self, expires: float) -> float:
        now = datetime.utcnow().timestamp()
        return self._timestamp_window(now - expires).start


class TumblingWindow(HoppingWindow):
    """Tumbling window type.

    Fixed-size, non-overlapping, gap-less windows.
    """

    def __init__(self, size: Seconds,
                 expires: Seconds = None) -> None:
        super(TumblingWindow, self).__init__(size, size, expires)


class SlidingWindow(WindowT):
    """Sliding window type.

    Fixed-size, overlapping windows that work on differences between
    record timestamps
    """

    before: float
    after: float

    def __init__(self, before: Seconds, after: Seconds,
                 expires: Seconds) -> None:
        self.before = want_seconds(before)
        self.after = want_seconds(after)
        self.expires = want_seconds(expires)

    def windows(self, timestamp: float) -> List[WindowRange]:
        """Return list of windows from timestamp.

        Notes:
            .. code-block:: text

                SELECT * FROM s1, s2
                WHERE
                    s1.key = s2.key
                AND
                s1.ts - before <= s2.ts AND s2.ts <= s1.ts + after
        """
        return [WindowRange(start=timestamp - self.before,
                            end=timestamp + self.after)]

    def stale_before(self) -> Optional[float]:
        return (
            self._stale_before(self.expires)
            if self.expires else None
        )

    @classmethod
    def _stale_before(cls, expires: float) -> float:
        return datetime.utcnow().timestamp() - expires
