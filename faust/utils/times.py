from datetime import timedelta
from functools import singledispatch
from typing import Union

__all__ = ['Seconds', 'want_seconds', 'want_milliseconds']

Seconds = Union[timedelta, float]


@singledispatch
def want_seconds(s: float) -> float:
    return s


@want_seconds.register(timedelta)
def _(s: timedelta) -> float:
    return s.total_seconds()


@singledispatch
def want_milliseconds(s: float) -> float:
    return s * 1000.0


@want_milliseconds.register(timedelta)
def _(s: timedelta) -> float:
    return want_milliseconds(s.total_seconds())
