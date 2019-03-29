"""LiveCheck - Models."""
from datetime import datetime, timezone
from typing import Any, Dict, List
from faust import Record
from mode.utils.objects import cached_property
from mode.utils.text import abbr

__all__ = ['SignalEvent', 'TestExecution']


class SignalEvent(Record):
    """Signal sent to test (see :class:`faust.livecheck.signals.Signal`)."""

    signal_name: str
    case_name: str
    key: Any
    value: Any


class TestExecution(Record):
    """Requested test excecution."""

    id: str
    case_name: str
    timestamp: float
    test_args: List[Any]
    test_kwargs: Dict[str, Any]

    @cached_property
    def ident(self) -> str:
        return self._build_ident(self.case_name, self.id)

    @cached_property
    def shortident(self) -> str:
        return self._build_ident(
            self.short_case_name,
            abbr(self.id, max=15, suffix='[...]'),
        )

    def _build_ident(self, name: str, id: str) -> str:
        return f'{name}:{id}'

    @cached_property
    def date(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp).astimezone(timezone.utc)

    @cached_property
    def human_date(self) -> str:
        date = self.date
        if date.date() == self._now().date():
            return 'Today ' + date.time().strftime('%H:%M:%S')
        return str(date)

    def _now(self) -> datetime:
        return datetime.now().astimezone(timezone.utc)

    @cached_property
    def short_case_name(self) -> str:
        return self.case_name.split('.')[-1]
