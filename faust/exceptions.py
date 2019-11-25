"""Faust exceptions."""
import typing

__all__ = [
    'FaustError',
    'FaustWarning',
    'FaustPredicate',
    'SecurityError',
    'NotReady',
    'Skip',
    'AlreadyConfiguredWarning',
    'ImproperlyConfigured',
    'ValidationError',
    'DecodeError',
    'KeyDecodeError',
    'ValueDecodeError',
    'SameNode',
    'ProducerSendError',
    'ConsumerNotStarted',
    'PartitionsMismatch',
    'ConsistencyError',
]

if typing.TYPE_CHECKING:
    from .types.models import FieldDescriptorT as _FieldDescriptorT
else:
    class _FieldDescriptorT: ...  # noqa


class FaustError(Exception):
    """Base-class for all Faust exceptions."""


class FaustWarning(UserWarning):
    """Base-class for all Faust warnings."""


class FaustPredicate(FaustError):
    """Base-class for semi-predicates such as :exc:`Skip`."""


class SecurityError(FaustError):
    """Base-class for security related (high priority) exceptions."""


class NotReady(FaustPredicate):
    """Service not started."""


class Skip(FaustPredicate):
    """Raised in stream processors to skip processing of an event."""


class AlreadyConfiguredWarning(FaustWarning):
    """Trying to configure app after configuration accessed."""


class ImproperlyConfigured(FaustError):
    """The library is not configured/installed correctly."""


class ValidationError(FaustError, ValueError):
    """Value passed for model field is not valid."""

    field: _FieldDescriptorT

    def __init__(self, reason: str, *,
                 field: _FieldDescriptorT) -> None:
        self.reason = reason
        self.field = field
        super().__init__(reason, field)

    def __str__(self) -> str:
        return f'{self.reason} {self.field!r}'

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self}>'


class DecodeError(FaustError):
    """Error while decoding/deserializing message key/value."""


class KeyDecodeError(DecodeError):
    """Error while decoding/deserializing message key."""


class ValueDecodeError(DecodeError):
    """Error while decoding/deserializing message value."""


class SameNode(FaustPredicate):
    """Exception raised by router when data is located on same node."""


class ProducerSendError(FaustError):
    """Error while sending attached messages prior to commit."""


class ConsumerNotStarted(NotReady):
    """Error trying to perform operation on consumer not started."""


class PartitionsMismatch(FaustError):
    """Number of partitions between related topics differ."""


class ConsistencyError(FaustError):
    """Persisted table offset is out of sync with changelog topic highwater."""
