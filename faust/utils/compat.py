"""Compatibility utilities."""

__all__ = ['want_bytes', 'want_str']


def want_bytes(s):
    """Convert string to bytes."""
    return s.encode() if isinstance(s, str) else s


def want_str(s):
    """Convert bytes to string."""
    return s.decode() if isinstance(s, bytes) else s
