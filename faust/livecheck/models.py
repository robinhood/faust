"""LiveCheck - Models."""
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional

from mode.utils.compat import want_str
from mode.utils.objects import cached_property
from mode.utils.text import abbr

from faust import Record
from faust.utils.iso8601 import parse as parse_iso8601

__all__ = ['State', 'SignalEvent', 'TestExecution', 'TestReport']

HEADER_TEST_ID = 'LiveCheck-Test-Id'
HEADER_TEST_NAME = 'LiveCheck-Test-Name'
HEADER_TEST_TIMESTAMP = 'LiveCheck-Test-Timestamp'
HEADER_TEST_EXPIRES = 'LiveCheck-Test-Expires'


class State(Enum):
    """Test execution status."""

    INIT = 'INIT'
    PASS = 'PASS'
    FAIL = 'FAIL'
    ERROR = 'ERROR'
    TIMEOUT = 'TIMEOUT'
    STALL = 'STALL'
    SKIP = 'SKIP'

    def is_ok(self) -> bool:
        """Return :const:`True` if this is considered an OK state."""
        return self in OK_STATES


OK_STATES = frozenset({State.INIT, State.PASS, State.SKIP})


class SignalEvent(Record):
    """Signal sent to test (see :class:`faust.livecheck.signals.Signal`)."""

    signal_name: str
    case_name: str
    key: Any
    value: Any


class TestExecution(Record, isodates=True):
    """Requested test execution."""

    id: str
    case_name: str
    timestamp: datetime
    test_args: List[Any]
    test_kwargs: Dict[str, Any]
    expires: datetime

    @classmethod
    def from_headers(cls, headers: Mapping) -> Optional['TestExecution']:
        """Create instance from mapping of HTTP/Kafka headers."""
        try:
            test_id = want_str(headers[HEADER_TEST_ID])
        except KeyError:
            return None
        else:
            test_name = headers[HEADER_TEST_NAME]
            timestamp = headers[HEADER_TEST_TIMESTAMP]
            expires = headers[HEADER_TEST_EXPIRES]

            return cls(
                id=test_id,
                case_name=want_str(test_name),
                timestamp=parse_iso8601(want_str(timestamp)),
                expires=parse_iso8601(want_str(expires)),
                test_args=(),
                test_kwargs={},
            )

    def as_headers(self) -> Mapping:
        """Return test metadata as mapping of HTTP/Kafka headers."""
        return {
            HEADER_TEST_ID: self.id,
            HEADER_TEST_NAME: self.case_name,
            HEADER_TEST_TIMESTAMP: self.timestamp.isoformat(),
            HEADER_TEST_EXPIRES: self.expires.isoformat(),
        }

    @cached_property
    def ident(self) -> str:
        """Return long identifier for this test used in logs."""
        return self._build_ident(self.case_name, self.id)

    @cached_property
    def shortident(self) -> str:
        """Return short identifier for this test used in logs."""
        return self._build_ident(
            self.short_case_name,
            abbr(self.id, max=15, suffix='[...]'),
        )

    def _build_ident(self, case_name: str, id: str) -> str:
        return f'{case_name}:{id}'

    def _now(self) -> datetime:
        return datetime.utcnow().astimezone(timezone.utc)

    @cached_property
    def human_date(self) -> str:
        """Return human-readable description of test timestamp."""
        if self.was_issued_today:
            return f'''Today {self.timestamp.strftime('%H:%M:%S')}'''
        else:
            return str(self.timestamp)

    @cached_property
    def was_issued_today(self) -> bool:
        """Return :const:`True` if test was issued on todays date."""
        return self.timestamp.date() == self._now().date()

    @cached_property
    def is_expired(self) -> bool:
        """Return :const:`True` if this test already expired."""
        return self._now() >= self.expires

    @cached_property
    def short_case_name(self) -> str:
        """Return abbreviated case name."""
        return self.case_name.split('.')[-1]


class TestReport(Record):
    """Report after test execution."""

    case_name: str
    state: State
    test: Optional[TestExecution]
    runtime: Optional[float]
    signal_latency: Dict[str, float]
    error: Optional[str]
    traceback: Optional[str]
