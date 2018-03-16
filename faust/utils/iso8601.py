try:
    import ciso8601
except ImportError:
    from ._iso8601_python import parse
else:
    parse = ciso8601.parse_datetime

__all__ = ['parse']
