from datetime import timedelta
from functools import singledispatch
from typing import Union

__all__ = ['Seconds', 'want_seconds']

#: Seconds can be expressed as float or :class:`~datetime.timedelta`,
Seconds = Union[timedelta, float]


@singledispatch
def want_seconds(s: float) -> float:
    """Convert :data:`Seconds` to float."""
    return s


@want_seconds.register(timedelta)
def _(s: timedelta) -> float:
    return s.total_seconds()
