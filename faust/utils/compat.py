"""Compatibility utilities."""
from typing import AnyStr, IO

__all__ = ['OrderedDict', 'want_bytes', 'want_str', 'isatty']

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


def isatty(fh: IO) -> bool:
    """Return True if fh has a controlling terminal.

    Notes:
        Use with e.g. :data:`sys.stdin`.
    """
    try:
        return fh.isatty()
    except AttributeError:
        return False
