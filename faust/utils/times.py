from datetime import timedelta
from typing import Union

__all__ = ['Seconds', 'want_seconds']

Seconds = Union[timedelta, float]


def want_seconds(s: Seconds) -> float:
    if isinstance(s, timedelta):
        return s.total_seconds()
    return s
