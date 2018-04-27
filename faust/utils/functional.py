"""Functional utilities."""
from itertools import groupby
from typing import Any, Callable, Iterable, Iterator, Optional, Sequence

__all__ = ['consecutive_numbers', 'maybe']


def consecutive_numbers(it: Iterable[int]) -> Iterator[Sequence[int]]:
    """Find runs of consecutive numbers.

    Notes:
        See https://docs.python.org/2.6/library/itertools.html#examples
    """
    for _, g in groupby(enumerate(it), lambda a: a[0] - a[1]):
        yield [a[1] for a in g]


def maybe(typ: Callable[[Any], Any], val: Optional[Any]) -> Optional[Any]:
    """Call typ on value if val is defined."""
    return typ(val) if val is not None else val
