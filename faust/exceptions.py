"""Faust exceptions."""

__all__ = [
    'FaustError',
    'FaustWarning',
    'AlreadyConfiguredWarning',
    'ImproperlyConfigured',
    'DecodeError',
    'KeyDecodeError',
    'ValueDecodeError',
]


class FaustError(Exception):
    """Base-class for all Faust exceptions."""


class FaustWarning(UserWarning):
    """Base-class for all Faust warnings."""


class AlreadyConfiguredWarning(FaustWarning):
    """Trying to configure app after configuration accessed."""


class ImproperlyConfigured(FaustError):
    """The library is not configured/installed correctly."""


class DecodeError(FaustError):
    """Error while decoding/deserializing message key/value."""


class KeyDecodeError(DecodeError):
    """Error while decoding/deserializing message key."""


class ValueDecodeError(DecodeError):
    """Error while decoding/deserializing message value."""


class SameNode(FaustError):
    """Exception raised by router when data is located on same node."""


class ProducerSendError(FaustError):
    """Error while sending attached messages prior to commit"""
