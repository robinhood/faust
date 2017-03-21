"""Exceptions."""


class ImproperlyConfigured(Exception):
    """The library is not configured/installed correctly."""


class KeyDecodeError(Exception):
    """Error while decoding/deserializing message key."""


class ValueDecodeError(Exception):
    """Error while decoding/deserialization message value."""
