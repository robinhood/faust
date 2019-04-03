"""LiveCheck - Models."""
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional

from mode.utils.compat import want_str
from mode.utils.objects import cached_property
from mode.utils.text import abbr

from faust import Record
from faust.utils.iso8601 import parse as parse_iso8601

__all__ = ['State', 'SignalEvent', 'TestExecution']

HEADER_TEST_ID = 'LiveCheck-Test-Id'
HEADER_TEST_NAME = 'LiveCheck-Test-Name'
HEADER_TEST_TIMESTAMP = 'LiveCheck-Test-Timestamp'
HEADER_TEST_EXPIRES = 'LiveCheck-Test-Expires'


class State(Enum):
    INIT = 'INIT'
    PASS = 'PASS'
    FAIL = 'FAIL'
    ERROR = 'ERROR'
    TIMEOUT = 'TIMEOUT'
    SKIP = 'SKIP'

    def is_ok(self) -> bool:
        return self in OK_STATES


OK_STATES = frozenset({State.INIT, State.PASS, State.SKIP})


class SignalEvent(Record):
    """Signal sent to test (see :class:`faust.livecheck.signals.Signal`)."""

    signal_name: str
    case_name: str
    key: Any
    value: Any


class TestExecution(Record, isodates=True):
    """Requested test excecution."""

    id: str
    case_name: str
    timestamp: datetime
    test_args: List[Any]
    test_kwargs: Dict[str, Any]
    expires: datetime

    @classmethod
    def from_headers(cls, headers: Mapping) -> Optional['TestExecution']:
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
        return {
            HEADER_TEST_ID: self.id,
            HEADER_TEST_NAME: self.case_name,
            HEADER_TEST_TIMESTAMP: self.timestamp.isoformat(),
            HEADER_TEST_EXPIRES: self.expires.isoformat(),
        }

    @cached_property
    def ident(self) -> str:
        return self._build_ident(self.case_name, self.id)

    @cached_property
    def shortident(self) -> str:
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
        if self.was_issued_today:
            return f'''Today {self.timestamp.strftime('%H:%M:%S')}'''
        else:
            return str(self.timestamp)

    @cached_property
    def was_issued_today(self) -> bool:
        return self.timestamp.date() == self._now().date()

    @cached_property
    def is_expired(self) -> bool:
        return self._now() >= self.expires

    @cached_property
    def short_case_name(self) -> str:
        return self.case_name.split('.')[-1]
