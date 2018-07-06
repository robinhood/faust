import logging
import operator
import typing
from typing import Any, Callable, Mapping, MutableMapping, cast
from mode import Service
from faust.types.sensors import SystemCheckT, SystemChecksT, T

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa

__all__ = ['Check', 'Condition', 'Increasing', 'SystemChecks']

CHECK_FREQUENCY = 5.0

STATE_OK = 'OK'
STATE_FAIL = 'FAIL'
STATE_SLOW = 'SLOW'
STATE_STALL = 'STALL'
STATE_UNASSIGNED = 'UNASSIGNED'
STATE_REBALANCING = 'REBALANCING'

OK_STATES = {STATE_OK, STATE_UNASSIGNED}
MAYBE_STATES = {STATE_REBALANCING}


class Check(SystemCheckT[T]):
    description: str = ''
    negate_description: str = ''

    state_to_severity = {
        STATE_SLOW: logging.WARNING,
        STATE_STALL: logging.ERROR,
        STATE_FAIL: logging.CRITICAL,
    }

    faults_to_state = [
        (6, STATE_STALL),
        (3, STATE_SLOW),
        (0, STATE_OK),
    ]

    def __init__(self,
                 name: str,
                 get_value: Callable[[], T] = None,
                 operator: Callable[[T, T], bool] = None) -> None:
        self.name = name
        self._get_value = cast(Callable[[], T], get_value)
        if operator is None:
            operator = self.default_operator
        self.operator = operator
        self.faults = 0
        self.prev_value = None
        self.state = STATE_OK

    def asdict(self) -> Mapping[str, Any]:
        return {
            'state': self.state,
            'color': self.color,
            'faults': self.faults,
        }

    def get_value(self) -> T:
        if self._get_value is not None:
            return self._get_value()
        raise NotImplementedError()

    def check(self, app: AppT) -> None:
        current_value: T = self.get_value()
        prev_value = self.prev_value
        severity = app.log.info
        if prev_value is not None:
            if self.operator(current_value, prev_value):
                self.faults += 1
                self.state = self.get_state_for_faults(self.faults)
                severity = self.state_to_severity.get(self.state, logging.INFO)
                self.on_failed_log(severity, app, prev_value, current_value)
            else:
                self.faults = 0
                self.state = STATE_OK
                self.on_ok_log(app, prev_value, current_value)
        self.prev_value = current_value

    def on_failed_log(self,
                      severity: int,
                      app: AppT,
                      prev_value: T,
                      current_value: T) -> None:
        app.log.log(severity,
                    '%s:%s not %s (x%s): was %s now %s',
                    app.conf.id, self.name, self.negate_description,
                    self.faults, prev_value, current_value)

    def on_ok_log(self, app: AppT, prev_value: T, current_value: T) -> None:
        app.log.info('%s:%s %s: was %s now %s',
                     app.conf.id, self.name, self.description,
                     prev_value, current_value)

    def get_state_for_faults(self, faults: int) -> str:
        for level, state in self.faults_to_state:
            if faults > level:
                return state
        return STATE_OK

    @property
    def color(self) -> str:
        if self.state in OK_STATES:
            return 'green'
        elif self.state in MAYBE_STATES:
            return 'yellow'
        return 'red'


class Increasing(Check[T]):
    default_operator = operator.le
    description = 'increasing'
    negate_description = 'not increasing'


def _transitioned_to_false(previous: bool, current: bool) -> bool:
    return not current


class Condition(Check[bool]):
    description = 'functional'
    negate_description = 'nonfunctional'

    default_operator = _transitioned_to_false

    faults_to_state = [
        (1, STATE_FAIL),
        (0, STATE_OK),
    ]


class Stationary(Check[T]):
    """Monitors a value that should stand still, i.e, not going up or down."""
    description = 'functional'
    negate_description = 'increasing'

    default_operator = operator.eq


class SystemChecks(SystemChecksT, Service):
    checks: MutableMapping[str, SystemCheckT]

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        self.checks = {}
        Service.__init__(self, **kwargs)

    def add(self, check: SystemCheckT) -> None:
        self.checks[check.name] = check

    def remove(self, name: str) -> None:
        self.checks.pop(name, None)

    @Service.task
    async def _system_check(self) -> None:
        app = self.app
        while not self.should_stop:
            await self.sleep(CHECK_FREQUENCY)
            if app.rebalancing or app.unassigned:
                continue
            for system_check in self.checks.values():
                system_check.check(app)
