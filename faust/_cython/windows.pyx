# cython: language_level=3
from datetime import timedelta
from libc.math cimport floor
from faust.types import WindowT


cdef double want_seconds(object o, object default) except *:
    if o is None and default is not None:
        return default
    if isinstance(o, timedelta):
        return o.total_seconds()
    return o


cdef class SlidingWindow:
    cdef public:
        double before
        double after
        double expires

    def __init__(self, object before, object, after,
                 object expires = 0.0):
        self.before = want_seconds(before, None)
        self.after = want_seconds(after, None)
        self.expires = want_seconds(expires, 0.0)

    cdef object ranges(self, double timestamp):
        return [
            (timestamp - self.before, timestamp + self.after),
        ]

    def stale(self, double timestamp, double latest_timestamp):
        if not self.expires:
            return False
        when_stale = latest_timestamp - self.expires
        return timestamp <= when_stale



cdef class HoppingWindow:

    cdef public:
        double size
        double step
        double expires

    def __init__(self, object size, object step,
                 object expires = 0.0):
        self.size = want_seconds(size, None)
        self.step = want_seconds(step, None)
        self.expires = want_seconds(expires, 0.0)

    cpdef object ranges(self, double timestamp):
        start = self._start_initial_range(timestamp)
        r = []
        for start in range(int(start), int(timestamp) + 1, int(self.step)):
            end = start + self.size - 0.1
            r.append((start, end))
        return r

    cdef double _start_initial_range(self, double timestamp):
        cdef:
            int rem
            double step
            double closest_step
        step = self.step
        rem = <int>floor(timestamp / step)
        closest_step = <double>rem * step
        return closest_step - self.size + step

    def stale(self, double timestamp, double latest_timestamp):
        cdef:
            double ts
        if not self.expires:
            return False
        ts = latest_timestamp - self.expires
        initial_start = self._start_initial_range(ts)
        when_stale = self._current_start(ts, initial_start)
        return timestamp <= when_stale

    cpdef object current(self, double timestamp):
        initial_start = self._start_initial_range(timestamp)
        start = self._current_start(timestamp, initial_start)
        end = start + self.size - 0.1
        return (start, end)

    cdef double _current_start(self, double timestamp, double start):
        cdef:
            double m
        step = self.step
        m = floor((timestamp - start) / step)
        return start + (step * m)

    cpdef delta(self, double timestamp, object d):
        return self.current(timestamp - want_seconds(d, None))

    cpdef object earliest(self, double timestamp):
        start = self._start_initial_range(timestamp)
        end = start + self.size - 0.1
        return (start, end)
