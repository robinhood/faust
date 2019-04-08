from collections import deque
import pytest
from faust.utils.functional import (
    consecutive_numbers,
    deque_pushpopmax,
)


@pytest.mark.parametrize('numbers,expected', [
    ([1, 2, 3, 4, 6, 7, 8], [1, 2, 3, 4]),
    ([1, 4, 6, 8, 10], [1]),
    ([1], [1]),
    ([103, 104, 105, 106, 100000000000], [103, 104, 105, 106]),
])
def test_consecutive_numbers(numbers, expected):
    assert next(consecutive_numbers(numbers), None) == expected


def test_deque_pushpop_max():
    d = deque([])
    deque_pushpopmax(d, 1, max=None)
    assert d == deque([1])
    deque_pushpopmax(d, 2, max=3)
    assert d == deque([1, 2])
    deque_pushpopmax(d, 3, max=3)
    assert d == deque([1, 2, 3])
    deque_pushpopmax(d, 4, max=3)
    assert d == deque([2, 3, 4])
    for i in range(5, 100):
        deque_pushpopmax(d, i, max=3)
        assert d == deque([i - 2, i - 1, i])
