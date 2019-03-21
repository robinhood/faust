"""Window Types."""
import os
from math import floor
from typing import List, Type, cast
from mode import Seconds, want_seconds
from .types.windows import WindowRange, WindowRange_from_start, WindowT

__all__ = [
    'Window',
    'HoppingWindow',
    'TumblingWindow',
    'SlidingWindow',
]

NO_CYTHON = bool(os.environ.get('NO_CYTHON', False))


class Window(WindowT):
    """Base class for window types."""


class _PyHoppingWindow(Window):
    """Hopping window type.

    Fixed-size, overlapping windows.
    """

    size: float
    step: float

    def __init__(self, size: Seconds, step: Seconds,
                 expires: Seconds = None) -> None:
        self.size = want_seconds(size)
        self.step = want_seconds(step)
        self.expires = want_seconds(expires) if expires else None

    def ranges(self, timestamp: float) -> List[WindowRange]:
        start = self._start_initial_range(timestamp)
        return [
            WindowRange_from_start(float(start), self.size)
            for start in range(int(start), int(timestamp) + 1, int(self.step))
        ]

    def stale(self, timestamp: float, latest_timestamp: float) -> bool:
        return (timestamp <= self._stale_before(latest_timestamp, self.expires)
                if self.expires else False)

    def current(self, timestamp: float) -> WindowRange:
        """Get the latest window range for a given timestamp."""
        step = self.step
        start = self._start_initial_range(timestamp)
        m = floor((timestamp - start) / step)
        return WindowRange_from_start(start + (step * m), self.size)

    def delta(self, timestamp: float, d: Seconds) -> WindowRange:
        return self.current(timestamp - want_seconds(d))

    def earliest(self, timestamp: float) -> WindowRange:
        start = self._start_initial_range(timestamp)
        return WindowRange_from_start(float(start), self.size)

    def _start_initial_range(self, timestamp: float) -> float:
        closest_step = (timestamp // self.step) * self.step
        return closest_step - self.size + self.step

    def _stale_before(self, latest_timestamp: float, expires: float) -> float:
        return self.current(latest_timestamp - expires)[0]


if not NO_CYTHON:  # pragma: no cover
    try:
        from ._cython.windows import HoppingWindow
    except ImportError:
        HoppingWindow = _PyHoppingWindow
    else:
        HoppingWindow = cast(Type[Window], HoppingWindow)
        # isinstance(HoppingWindow, Window) is True
        # isinstance(HoppingWindow, WindowT) is True
        Window.register(HoppingWindow)
else:  # pragma: no cover
    HoppingWindow = _PyHoppingWindow


class TumblingWindow(HoppingWindow):
    """Tumbling window type.

    Fixed-size, non-overlapping, gap-less windows.
    """

    def __init__(self, size: Seconds, expires: Seconds = None) -> None:
        super(TumblingWindow, self).__init__(size, size, expires)


class _PySlidingWindow(Window):
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

    def ranges(self, timestamp: float) -> List[WindowRange]:
        """Return list of windows from timestamp.

        Notes:
            .. sourcecode:: sql

                SELECT *
                  FROM s1, s2
                WHERE
                    s1.key = s2.key
                AND
                s1.ts - before <= s2.ts AND s2.ts <= s1.ts + after

        """
        return [
            (timestamp - self.before, timestamp + self.after),
        ]

    def stale(self, timestamp: float, latest_timestamp: float) -> bool:
        return (timestamp <= self._stale_before(self.expires, latest_timestamp)
                if self.expires else False)

    def _stale_before(self, expires: float, latest_timestamp: float) -> float:
        return latest_timestamp - expires

    def current(self, timestamp: float) -> WindowRange:
        """Get the latest window range for a given timestamp."""
        return timestamp - self.before, timestamp + self.after

    def delta(self, timestamp: float, d: Seconds) -> WindowRange:
        return self.current(timestamp - want_seconds(d))

    def earliest(self, timestamp: float) -> WindowRange:
        return self.current(timestamp)


if not NO_CYTHON:  # pragma: no cover
    try:
        from ._cython.windows import SlidingWindow
    except ImportError:
        SlidingWindow = _PySlidingWindow
    else:
        SlidingWindow = cast(Type[Window], SlidingWindow)
        Window.register(SlidingWindow)
else:  # pragma: no cover
    SlidingWindow = _PySlidingWindow
