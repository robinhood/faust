"""Utilities."""
from uuid import uuid4

__all__ = ['uuid']


def uuid() -> str:
    """Generate random UUID string.

    Shortcut to ``str(uuid4())``.
    """
    return str(uuid4())
