"""Faust exceptions."""

__all__ = [
    'FaustError',
    'FaustWarning',
    'AlreadyConfiguredWarning',
    'ImproperlyConfigured',
    'DecodeError',
    'KeyDecodeError',
    'ValueDecodeError',
    'ConsumerNotStarted',
    'PartitionsMismatch',
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


class ConsumerNotStarted(FaustError):
    """Error trying to perform operation on consumer not started."""


class PartitionsMismatch(FaustError):
    """Number of partitions between related topics differ."""


class ConsistencyError(FaustError):
    """Persisted table offset is out of sync with changelog topic highwater."""
