import logging
import operator
import socket
from contextlib import contextmanager
from typing import Any, Callable, Mapping, MutableMapping, Optional, cast
from mode import Service
from faust.types import AppT
from . import states
from .app import send_update
from .models import Status

CHECK_FREQUENCY = 5.0


# Note: Check is a serviceproxy since Service.state already exists.
class Check(Service):
    description: str = ''

    # This can be used to format the "current value" in error logs.
    # If not set it will just use ``repr(value)``.
    prev_value_repr: Optional[str] = None
    current_value_repr: Optional[str] = None

    state_to_severity = {
        states.SLOW: logging.WARNING,
        states.STALL: logging.ERROR,
        states.FAIL: logging.CRITICAL,
    }

    faults_to_state = [
        (6, states.STALL),
        (3, states.SLOW),
        (0, states.OK),
    ]

    default_operator = None

    def __init__(self,
                 name: str,
                 get_value: Callable[[], Any] = None,
                 operator: Callable[[Any, Any], bool] = None,
                 **kwargs: Any):
        self.name = name
        self._get_value = cast(Callable[[], Any], get_value)
        if operator is None:
            operator = self.default_operator
        self.operator = operator
        self.faults = 0
        self.prev_value = None
        self.status = states.OK
        self.interval_skew = 0.0
        self.app = None
        super().__init__(**kwargs)

    def to_representation(self, app, severity) -> Status:
        return Status(
            app_id=app.conf.id,
            hostname=socket.gethostname(),
            category=self.name,
            color=self.color,
            count=self.faults,
            state=self.status,
            severity=logging.getLevelName(severity),
        )

    def asdict(self) -> Mapping[str, Any]:
        return {
            'state': self.status,
            'color': self.color,
            'faults': self.faults,
        }

    def get_value(self) -> Any:
        if self._get_value is not None:
            return self._get_value()
        raise NotImplementedError()

    async def on_rebalancing(self, app: AppT):
        self.status = states.REBALANCING
        await send_update(app, self.to_representation(app, logging.INFO))

    async def on_unassigned(self, app: AppT):
        self.status = states.UNASSIGNED
        await send_update(app, self.to_representation(app, logging.INFO))

    async def on_paused(self, app: AppT):
        self.status = states.PAUSED
        await send_update(app, self.to_representation(app, logging.INFO))

    async def check(self, app: AppT) -> None:
        current_value: Any = self.get_value()
        prev_value = self.prev_value
        severity = app.log.info
        try:
            if prev_value is not None:
                if self.compare(prev_value, current_value):
                    self.faults += 1
                    self.status = self.get_state_for_faults(self.faults)
                    severity = self.state_to_severity.get(
                        self.status, logging.INFO)
                    await self.on_failed_log(
                        severity, app, prev_value, current_value)
                else:
                    self.faults = 0
                    self.status = states.OK
                    await self.on_ok_log(app, prev_value, current_value)
            self.store_previous_value(current_value)
        except Exception as exc:
            print('ERROR: %r' % (exc,))
            raise

    def compare(self, prev_value: Any, current_value: Any):
        return self.operator(current_value, prev_value)

    def store_previous_value(self, current_value):
        self.prev_value = current_value

    async def on_failed_log(self,
                            severity: int,
                            app: AppT,
                            prev_value: Any,
                            current_value: Any) -> None:
        await send_update(app, self.to_representation(app, severity))
        prev_value_repr = self.prev_value_repr
        current_value_repr = self.current_value_repr
        if current_value_repr is None:
            current_value_repr = repr(current_value)
        if prev_value_repr is None:
            prev_value_repr = repr(prev_value)

        app.log.log(severity,
                    '%s:%s %s (x%s): was %s now %s',
                    app.conf.id, self.name, self.negate_description,
                    self.faults, prev_value_repr, current_value_repr)

    async def on_ok_log(self,
                        app: AppT,
                        prev_value: Any,
                        current_value: Any) -> None:
        await send_update(app, self.to_representation(app, logging.INFO))
        app.log.info('%s:%s %s: was %s now %s',
                     app.conf.id, self.name, self.description,
                     prev_value, current_value)

    def get_state_for_faults(self, faults: int) -> str:
        for level, state in self.faults_to_state:
            if faults > level:
                return state
        return states.OK

    @property
    def color(self) -> str:
        if self.status in states.OK_STATES:
            return 'green'
        elif self.status in states.MAYBE_STATES:
            return 'yellow'
        return 'red'

    @Service.task
    async def _run_check(self) -> None:
        try:
            app = self.app
            while not self.should_stop:
                await self.sleep(CHECK_FREQUENCY)
                if app.system_checks.paused:
                    await self.on_paused(app)
                elif app.rebalancing:
                    await self.on_rebalancing(app)
                elif app.unassigned:
                    await self.on_unassigned(app)
                else:
                    await self.check(app)
        except Exception as exc:
            print('RUN CHECK RAISED: %r' % (exc,))
            raise

    @property
    def label(self) -> str:
        return f'{type(self).__name__}: {self.name}'


class Increasing(Check):
    default_operator = operator.le
    description = 'increasing'
    negate_description = 'not increasing'


def _transitioned_to_false(previous: bool, current: bool) -> bool:
    return not current


class Condition(Check):
    description = 'functional'
    negate_description = 'nonfunctional'

    default_operator = _transitioned_to_false

    faults_to_state = [
        (1, states.FAIL),
        (0, states.OK),
    ]


class Stationary(Check):
    """Monitors a value that should stand still, i.e, not going up or down."""
    description = 'functional'
    negate_description = 'increasing'

    default_operator = operator.ne


class SystemChecks(Service):
    checks: MutableMapping[str, Check]
    current_skew = 0.0
    paused: bool = False

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        self.checks = {}
        Service.__init__(self, **kwargs)

    def on_init_dependencies(self):
        return self.checks.values()

    @contextmanager
    def pause(self):
        self.paused = True
        try:
            yield
        finally:
            self.paused = False

    def add(self, check: Check) -> None:
        self.checks[check.name] = check
        self.current_skew += 0.2
        check.interval_skew = self.current_skew
        check.app = self.app

    def remove(self, name: str) -> None:
        self.checks.pop(name, None)
