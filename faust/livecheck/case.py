"""LiveCheck - Test cases."""
import traceback
import typing

from collections import deque
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from itertools import count
from random import uniform
from statistics import median
from time import monotonic
from typing import (
    Any,
    ClassVar,
    Dict,
    Iterable,
    Optional,
    Type,
    Union,
    cast,
)

from aiohttp import ClientError, ClientTimeout
from mode import Seconds, Service, want_seconds
from mode.utils.contexts import asynccontextmanager
from mode.utils.times import humanize_seconds
from mode.utils.typing import AsyncGenerator, Counter, Deque
from yarl import URL

from faust.utils import uuid
from faust.utils.functional import deque_pushpopmax

from .exceptions import ServiceDown, SuiteFailed, SuiteStalled
from .locals import current_execution_stack, current_test_stack
from .models import SignalEvent, State, TestExecution, TestReport
from .runners import TestRunner
from .signals import BaseSignal

if typing.TYPE_CHECKING:
    from .app import LiveCheck as _LiveCheck
else:
    class _LiveCheck: ...  # noqa

__all__ = ['Case']


class Case(Service):
    """LiveCheck test case."""

    Runner: ClassVar[Type[TestRunner]] = TestRunner

    app: _LiveCheck

    #: Name of the test
    #: If not set this will be generated out of the subclass name.
    name: str

    active: bool = True

    #: Current state of this test.
    status: State = State.INIT

    #: How often we execute the test using fake data
    #: (define Case.make_fake_request()).
    #:
    #: Set to None if production traffic is frequent enough to
    #: satisfy :attr:`warn_stalled_after`.
    frequency: Optional[float] = None

    #: Timeout in seconds for when after we warn that nothing is processing.
    warn_stalled_after: float = 1800.0

    #: Probability of test running when live traffic is going through.
    probability: float = 0.5

    max_consecutive_failures: int = 30

    #: The warn_stalled_after timer uses this to keep track of
    #: either when a test was last received, or the last time the timer
    #: timed out.
    last_test_received: Optional[float] = None

    #: Timestamp of when the suite last failed.
    last_fail: Optional[float] = None

    #: Max items to store in :attr:`latency_history` and
    #: :attr:`runtime_history`.
    max_history: int = 100
    latency_history: Deque[float]
    frequency_history: Deque[float]
    runtime_history: Deque[float]

    runtime_avg: Optional[float] = None
    latency_avg: Optional[float] = None
    frequency_avg: Optional[float] = None

    signals: Dict[str, BaseSignal]
    _original_signals: Iterable[BaseSignal]

    total_signals: int
    test_expires: timedelta = timedelta(hours=3)

    realtime_logs = False

    url_timeout_total: Optional[float] = 5 * 60.0
    url_timeout_connect: Optional[float] = None
    url_error_retries: int = 10
    url_error_delay_min: float = 0.5
    url_error_delay_backoff: float = 1.5
    url_error_delay_max: float = 5.0

    state_transition_delay: float = 60.0

    consecutive_failures: int = 0
    total_failures: int = 0
    total_by_state: Counter[State]

    def __init__(self, *,
                 app: _LiveCheck,
                 name: str,
                 probability: float = None,
                 warn_stalled_after: Seconds = None,
                 active: bool = None,
                 signals: Iterable[BaseSignal] = None,
                 test_expires: Seconds = None,
                 frequency: Seconds = None,
                 realtime_logs: bool = None,
                 max_history: int = None,
                 max_consecutive_failures: int = None,
                 url_timeout_total: float = None,
                 url_timeout_connect: float = None,
                 url_error_retries: int = None,
                 url_error_delay_min: float = None,
                 url_error_delay_backoff: float = None,
                 url_error_delay_max: float = None,
                 **kwargs: Any) -> None:
        self.app = app
        self.name = name
        if active is not None:
            self.active = active
        if probability is not None:
            self.probability = probability
        if warn_stalled_after is not None:
            self.warn_stalled_after = want_seconds(warn_stalled_after)
        self._original_signals = signals or ()
        self.signals = {
            sig.name: sig.clone(case=self)
            for sig in self._original_signals
        }
        if test_expires is not None:
            self.test_expires = timedelta(seconds=want_seconds(test_expires))
        if frequency is not None:
            self.frequency = want_seconds(frequency)
        if realtime_logs is not None:
            self.realtime_logs = realtime_logs
        if max_history is not None:
            self.max_history = max_history
        if max_consecutive_failures is not None:
            self.max_consecutive_failures = max_consecutive_failures

        if url_timeout_total is not None:
            self.url_timeout_total = url_timeout_total
        if url_timeout_connect is not None:
            self.url_timeout_connect = url_timeout_connect
        if url_error_retries is not None:
            self.url_error_retries = url_error_retries
        if url_error_delay_min is not None:
            self.url_error_delay_min = url_error_delay_min
        if url_error_delay_backoff is not None:
            self.url_error_delay_backoff = url_error_delay_backoff
        if url_error_delay_max is not None:
            self.url_error_delay_max = url_error_delay_max

        self.frequency_history = deque()
        self.latency_history = deque()
        self.runtime_history = deque()

        self.total_by_state = Counter()

        self.total_signals = len(self.signals)
        # update local attribute so that the
        # signal attributes have the correct signal instance.
        self.__dict__.update(self.signals)

        Service.__init__(self, **kwargs)

    @Service.timer(10.0)
    async def _sampler(self) -> None:
        await self._sample()

    async def _sample(self) -> None:
        if self.frequency_history:
            self.frequency_avg = median(self.frequency_history)
        if self.latency_history:
            self.latency_avg = median(self.latency_history)
        if self.runtime_history:
            self.runtime_avg = median(self.runtime_history)

        self.log.info('Stats: (median) frequency=%r latency=%r runtime=%r',
                      self.frequency_avg, self.latency_avg, self.runtime_avg)

    @asynccontextmanager
    async def maybe_trigger(
            self, id: str = None,
            *args: Any,
            **kwargs: Any) -> AsyncGenerator[Optional[TestExecution], None]:
        """Schedule test execution, or not, based on probability setting."""
        execution: Optional[TestExecution] = None
        with ExitStack() as exit_stack:
            if uniform(0, 1) < self.probability:
                execution = await self.trigger(id, *args, **kwargs)
                exit_stack.enter_context(current_test_stack.push(execution))
            yield execution

    async def trigger(self, id: str = None,
                      *args: Any,
                      **kwargs: Any) -> TestExecution:
        """Schedule test execution ASAP."""
        id = id or uuid()
        execution = TestExecution(
            id=id,
            case_name=self.name,
            timestamp=self._now(),
            test_args=args,
            test_kwargs=kwargs,
            expires=self._now() + self.test_expires,
        )
        await self.app.pending_tests.send(key=id, value=execution)
        return execution

    def _now(self) -> datetime:
        return datetime.utcnow().astimezone(timezone.utc)

    async def run(self, *test_args: Any, **test_kwargs: Any) -> None:
        """Override this to define your test case."""
        raise NotImplementedError('Case class must implement run')

    async def resolve_signal(self, key: str, event: SignalEvent) -> None:
        """Mark test execution signal as resolved."""
        await self.signals[event.signal_name].resolve(key, event)

    async def execute(self, test: TestExecution) -> None:
        """Execute test using :class:`TestRunner`."""
        t_start = monotonic()
        runner = self.Runner(self, test, started=t_start)
        with current_execution_stack.push(runner):
            # resolve_models
            await runner.execute()

    async def on_test_start(self, runner: TestRunner) -> None:
        """Call when a test starts executing."""
        started = runner.started
        t_prev, self.last_test_received = self.last_test_received, started
        if t_prev:
            time_since = started - t_prev
            wanted_frequency = self.frequency
            if wanted_frequency:
                latency = time_since - wanted_frequency
                deque_pushpopmax(
                    self.latency_history, latency, self.max_history)
            deque_pushpopmax(
                self.frequency_history, time_since, self.max_history)

    async def on_test_skipped(self, runner: TestRunner) -> None:
        """Call when a test is skipped."""
        # wait until we have fast forwarded before raising errors
        # XXX should we use seek, or warn somehow if this
        # takes too long?
        self.last_test_received = monotonic()

    async def on_test_failed(self,
                             runner: TestRunner,
                             exc: BaseException) -> None:
        """Call when invariant in test execution fails."""
        await self._set_test_error_state(State.FAIL)

    async def on_test_error(self,
                            runner: TestRunner,
                            exc: BaseException) -> None:
        """Call when a test execution raises an exception."""
        await self._set_test_error_state(State.ERROR)

    async def on_test_timeout(self,
                              runner: TestRunner,
                              exc: BaseException) -> None:
        """Call when a test execution times out."""
        await self._set_test_error_state(State.TIMEOUT)

    async def _set_test_error_state(self, state: State) -> None:
        self.status = state
        self.consecutive_failures += 1
        self.total_failures += 1
        self.total_by_state[state] += 1
        if self.consecutive_failures >= self.max_consecutive_failures:
            try:
                raise SuiteFailed(
                    'Failed after {0!r} (max={1!r})'.format(
                        self.consecutive_failures,
                        self.max_consecutive_failures))
            except SuiteFailed as exc:
                await self.on_suite_fail(exc)

    def _set_pass_state(self, state: State) -> None:
        assert state.is_ok()
        self.status = state
        self.consecutive_failures = 0
        self.total_by_state[state] += 1

    async def on_test_pass(self, runner: TestRunner) -> None:
        """Call when a test execution passes."""
        test = runner.test
        runtime: float = runner.runtime or 0.0
        deque_pushpopmax(self.runtime_history, runtime, self.max_history)
        ts = test.timestamp.timestamp()
        last_fail = self.last_fail
        if last_fail is None or ts > last_fail:
            self._maybe_recover_from_failed_state()

    async def post_report(self, report: TestReport) -> None:
        """Publish test report."""
        await self.app.post_report(report)

    @Service.task
    async def _send_frequency(self) -> None:
        freq = self.frequency
        if freq:
            async for sleep_time in self.itertimer(
                    freq, name=f'{self.name}_send'):
                if self.app.is_leader():
                    await self.make_fake_request()

    async def make_fake_request(self) -> None:
        ...

    @Service.task
    async def _check_frequency(self) -> None:
        timeout = self.warn_stalled_after
        await self.sleep(timeout)
        self.last_test_received = None
        time_start = monotonic()
        last_warning: Optional[float] = None
        async for sleep_time in self.itertimer(
                timeout, name=f'{self.name}._wempty'):
            try:
                now = monotonic()
                can_warn = now - last_warning if last_warning else True
                if can_warn:
                    if self.last_test_received is not None:
                        secs_since = now - self.last_test_received
                    else:
                        secs_since = now - time_start
                    if secs_since > self.warn_stalled_after:
                        human_secs = humanize_seconds(secs_since)
                        # we reset the timer to avoid logging every second.
                        last_warning = now
                        raise SuiteStalled(
                            f'Test stalled! Last received {human_secs} ago '
                            f'(warn_stalled_after={timeout}).')
                    else:
                        self._maybe_recover_from_failed_state()
            except SuiteStalled as exc:
                # we don't want to propagate this here, keep running...
                await self.on_suite_fail(exc, State.STALL)

    async def on_suite_fail(self,
                            exc: SuiteFailed,
                            new_state: State = State.FAIL) -> None:
        """Call when the suite fails."""
        assert isinstance(exc, SuiteFailed)
        delay = self.state_transition_delay
        if self.status.is_ok() or self._failed_longer_than(delay):
            self.status = new_state
            self.last_fail = monotonic()
            self.log.exception(str(exc))
            await self.post_report(TestReport(
                case_name=self.name,
                state=new_state,
                test=None,
                runtime=None,
                signal_latency={},
                error=str(exc),
                traceback='\n'.join(traceback.format_tb(exc.__traceback__)),
            ))
        else:
            self.status = new_state
            self.last_fail = monotonic()

    def _maybe_recover_from_failed_state(self) -> None:
        if self.status != State.PASS:
            if self._failed_longer_than(self.state_transition_delay):
                self._set_pass_state(State.PASS)

    def _failed_longer_than(self, secs: float) -> bool:
        secs_since_fail = self.seconds_since_last_fail
        if secs_since_fail is None:
            return True
        else:
            return secs_since_fail > secs

    @property
    def seconds_since_last_fail(self) -> Optional[float]:
        """Return number of seconds since any test failed."""
        last_fail = self.last_fail
        return monotonic() - last_fail if last_fail else None

    async def get_url(self, url: Union[str, URL],
                      **kwargs: Any) -> Optional[bytes]:
        """Perform GET request using HTTP client."""
        return await self.url_request('get', url, **kwargs)

    async def post_url(self, url: Union[str, URL],
                       **kwargs: Any) -> Optional[bytes]:
        """Perform POST request using HTTP client."""
        return await self.url_request('post', url, **kwargs)

    async def url_request(self, method: str, url: Union[str, URL],
                          **kwargs: Any) -> Optional[bytes]:
        """Perform URL request using HTTP client."""
        timeout = ClientTimeout(
            # mypy thinks this must be float, but it can be None.
            total=cast(float, self.url_timeout_total),
            connect=cast(float, self.url_timeout_connect),
        )
        error_delay = self.url_error_delay_min
        try:
            for i in count():
                try:
                    async with self.app.http_client.request(
                            method, url,
                            timeout=timeout, **kwargs) as response:
                        response.raise_for_status()
                        payload = await response.read()
                        self._maybe_recover_from_failed_state()
                        return payload
                except ClientError as exc:
                    if i >= self.url_error_retries:
                        raise ServiceDown(
                            f'Cannot send fake test request: {exc!r}')
                    retry_in = humanize_seconds(
                        error_delay, microseconds=True)
                    self.log.warning('URL %r raised: %r (Will retry in %s)',
                                     url, exc, retry_in)
                    error_delay = min(
                        error_delay * self.url_error_delay_backoff,
                        self.url_error_delay_max,
                    )
                    await self.sleep(error_delay)
            else:  # pragma: no cover
                pass
        except ServiceDown as exc:
            # we don't want to propagate this here, keep running...
            await self.on_suite_fail(exc)
        return None

    @property
    def current_test(self) -> Optional[TestExecution]:
        """Return the currently active test in this task (if any)."""
        return current_test_stack.top

    @property
    def current_execution(self) -> Optional[TestRunner]:
        """Return the currently executing :class:`TestRunner` in this task."""
        return current_execution_stack.top

    @property
    def label(self) -> str:
        """Return human-readable label for this test case."""
        return f'{type(self).__name__}: {self.name}'
