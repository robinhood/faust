"""LiveCheck - related exceptions."""

__all__ = [
    'LiveCheckError',
    'TestFailed',
    'TestRaised',
    'TestTimeout',
]


class LiveCheckError(Exception):
    """Generic base class for livecheck test errors."""


class TestFailed(LiveCheckError):
    """The test failed an assertion."""


class TestRaised(LiveCheckError):
    """The test raised an exception."""


class TestTimeout(LiveCheckError):
    """The test timed out waiting for an event or during processing."""
