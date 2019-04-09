"""Functional utilities."""
from itertools import groupby
from typing import Iterable, Iterator, Optional, Sequence, TypeVar
from mode.utils.typing import Deque

__all__ = ['consecutive_numbers', 'deque_prune', 'deque_pushpopmax']

T = TypeVar('T')


def consecutive_numbers(it: Iterable[int]) -> Iterator[Sequence[int]]:
    """Find runs of consecutive numbers.

    Notes:
        See https://docs.python.org/2.6/library/itertools.html#examples
    """
    for _, g in groupby(enumerate(it), lambda a: a[0] - a[1]):
        yield [a[1] for a in g]


def deque_prune(l: Deque[T], max: int = None) -> Optional[T]:
    """Prune oldest element in deque if size exceeds ``max``."""
    if max is not None:
        size = len(l)
        if size > max:
            return l.popleft()
    return None


def deque_pushpopmax(l: Deque[T], item: T, max: int = None) -> Optional[T]:
    """Append to deque and remove oldest element if size exceeds ``max``."""
    l.append(item)
    return deque_prune(l, max)
