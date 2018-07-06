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

__all__ = ['Check', 'Increasing', 'SystemChecks']

CHECK_FREQUENCY = 5.0

STATE_OK = 'OK'
STATE_SLOW = 'SLOW'
STATE_STALL = 'STALL'
STATE_UNASSIGNED = 'UNASSIGNED'
STATE_REBALANCING = 'REBALANCING'

OK_STATES = {STATE_OK, STATE_UNASSIGNED}
MAYBE_STATES = {STATE_REBALANCING}


class Check(SystemCheckT[T]):

    state_to_severity = {
        STATE_SLOW: logging.WARNING,
        STATE_STALL: logging.ERROR,
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
                app.log.log(severity,
                            '%s:%s not progressing (x%s): was %s now %s',
                            app.conf.id, self.name,
                            self.faults, prev_value, current_value)
            else:
                self.faults = 0
                self.state = STATE_OK
                app.log.info('%s:%s progressing: was %s now %s',
                             app.conf.id, self.name, prev_value,
                             current_value)
        self.prev_value = current_value

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
            if not app.consumer.assignment():
                app.unassigned = True
                continue
            elif app.rebalancing:
                continue
            app.unassigned = False
            for system_check in self.checks.values():
                system_check.check(app)
