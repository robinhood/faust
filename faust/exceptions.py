"""Faust exceptions."""

__all__ = [
    'FaustError',
    'ImproperlyConfigured',
    'KeyDecodeError',
    'ValueDecodeError',
]


class FaustError(Exception):
    """Base-class for all Faust exceptions."""


class ImproperlyConfigured(FaustError):
    """The library is not configured/installed correctly."""


class KeyDecodeError(FaustError):
    """Error while decoding/deserializing message key."""


class ValueDecodeError(FaustError):
    """Error while decoding/deserialization message value."""


class MaxRestartsExceeded(FaustError):
    """Service was restarted too many times."""
