"""Cache-related errors."""

__all__ = ['CacheUnavailable']


class CacheUnavailable(Exception):
    """The cache is currently unavailable."""
