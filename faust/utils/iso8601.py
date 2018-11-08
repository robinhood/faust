try:  # pragma: no cover
    import ciso8601
except ImportError:  # pragma: no cover
    from ._iso8601_python import parse
else:
    parse = ciso8601.parse_datetime  # type: ignore

__all__ = ['parse']
