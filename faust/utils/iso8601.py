"""Parsing ISO-8601 string and converting to :class:`~datetime.datetime`."""
try:  # pragma: no cover
    import ciso8601
except ImportError:  # pragma: no cover
    from ._iso8601_python import parse
else:
    parse = ciso8601.parse_datetime

__all__ = ['parse']
