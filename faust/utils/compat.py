"""Compatibility utilities."""
from typing import AnyStr

__all__ = ['OrderedDict', 'want_bytes', 'want_str']

#: Dictionaries are ordered by default in Python 3.6
OrderedDict = dict


def want_bytes(s: AnyStr) -> bytes:
    """Convert string to bytes."""
    if isinstance(s, str):
        return s.encode()
    return s


def want_str(s: AnyStr) -> str:
    """Convert bytes to string."""
    if isinstance(s, bytes):
        return s.decode()
    return s
