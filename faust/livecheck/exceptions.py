"""LiveCheck - related exceptions."""

__all__ = [
    'LiveCheckError',
    'TestFailed',
    'TestRaised',
    'TestTimeout',
]


class LiveCheckError(Exception):
    """Generic base class for livecheck test errors."""


class SuiteFailed(LiveCheckError):
    """The whole test suite failed (not just a test).

    Also raised when ``warn_empty_after=3600`` is set and there has not
    been any execution requests in the last hour.
    """


class TestSkipped(LiveCheckError):
    """Test was skipped."""


class TestFailed(LiveCheckError):
    """The test failed an assertion."""


class TestRaised(LiveCheckError):
    """The test raised an exception."""


class TestTimeout(LiveCheckError):
    """The test timed out waiting for an event or during processing."""
