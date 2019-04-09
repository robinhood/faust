"""LiveCheck - related exceptions."""

__all__ = [
    'LiveCheckError',
    'SuiteFailed',
    'ServiceDown',
    'SuiteStalled',
    'TestSkipped',
    'TestFailed',
    'TestRaised',
    'TestTimeout',
]


class LiveCheckError(Exception):
    """Generic base class for LiveCheck test errors."""


class SuiteFailed(LiveCheckError):
    """The whole test suite failed (not just a test)."""


class ServiceDown(SuiteFailed):
    """Suite failed after a depending service is not responding.

    Used when for example a test case is periodically sending
    requests to a HTTP service, and that HTTP server is not responding.
    """


class SuiteStalled(SuiteFailed):
    """The suite is not running.

    Raised when ``warn_stalled_after=3600`` is set and there has not
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
