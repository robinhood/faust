"""LiveCheck - Test runner."""
import asyncio
import logging
import traceback
import typing

from time import monotonic
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
)

from mode.utils.logging import CompositeLogger
from mode.utils.times import humanize_seconds
from mode.utils.typing import NoReturn

from faust.models import maybe_model

from .exceptions import (
    LiveCheckError,
    TestFailed,
    TestRaised,
    TestSkipped,
    TestTimeout,
)
from .locals import current_test_stack
from .models import State, TestExecution, TestReport
from .signals import BaseSignal

if typing.TYPE_CHECKING:
    from .case import Case as _Case
else:
    class _Case: ...  # noqa

__all__ = ['TestRunner']


class TestRunner:
    """Execute and keep track of test instance."""

    case: _Case

    state: State = State.INIT
    test: TestExecution

    started: float
    ended: Optional[float]
    runtime: Optional[float]

    logs: List[Tuple[str, Tuple]]
    signal_latency: Dict[str, float]

    report: Optional[TestReport] = None
    error: Optional[BaseException] = None

    def __init__(self,
                 case: _Case,
                 test: TestExecution,
                 started: float) -> None:
        self.case = case
        self.test = test
        self.started = started
        self.ended = None
        self.runtime = None
        self.logs = []
        self.log = CompositeLogger(
            self.case.log.logger,
            formatter=self._format_log,
        )
        self.signal_latency = {}

    async def execute(self) -> None:
        """Execute this test."""
        test = self.test
        with current_test_stack.push(test):
            if not self.case.active:
                await self.skip('case inactive')
            elif test.is_expired:
                await self.skip('expired')
            args = self._prepare_args(test.test_args)
            kwargs = self._prepare_kwargs(test.test_kwargs)
            await self.on_start()
            try:
                await self.case.run(*args, **kwargs)
            except asyncio.CancelledError:
                pass
            except TestSkipped as exc:
                await self.on_skipped(exc)
                raise
            except TestTimeout as exc:
                await self.on_timeout(exc)
                raise
            except AssertionError as exc:
                await self.on_failed(exc)
                raise TestFailed(exc) from exc
            except LiveCheckError as exc:
                await self.on_error(exc)
                raise
            except Exception as exc:
                await self.on_error(exc)
                raise TestRaised(exc) from exc
            else:
                await self.on_pass()

    async def skip(self, reason: str) -> NoReturn:
        """Skip this test execution."""
        exc = TestSkipped(f'Test {self.test.ident} skipped: {reason}')
        try:
            raise exc
        except TestSkipped as exc:
            # save with traceback
            await self.on_skipped(exc)
            raise
        else:  # pragma: no cover
            assert False  # can not get here

    def _prepare_args(self, args: Iterable) -> Tuple:
        to_value = self._prepare_val
        return tuple(to_value(arg) for arg in args)

    def _prepare_kwargs(self, kwargs: Mapping[str, Any]) -> Mapping[str, Any]:
        to_value = self._prepare_val
        return {
            to_value(k): to_value(v)
            for k, v in kwargs.items()
        }

    def _prepare_val(self, arg: Any) -> Any:
        return maybe_model(arg)

    def _format_log(self, severity: int, msg: str,
                    *args: Any, **kwargs: Any) -> str:
        return f'[{self.test.shortident}] {msg}'

    async def on_skipped(self, exc: TestSkipped) -> None:
        """Call when a test execution was skipped."""
        self.state = State.SKIP
        self.log.info('Skipped expired test: %s expires=%s',
                      self.test.ident, self.test.expires)
        await self.case.on_test_skipped(self)

    async def on_start(self) -> None:
        """Call when a test starts executing."""
        self.log_info('≈≈≈ Test %s executing... (issued %s) ≈≈≈',
                      self.case.name, self.test.human_date)
        await self.case.on_test_start(self)

    async def on_signal_wait(self, signal: BaseSignal, timeout: float) -> None:
        """Call when the test is waiting for a signal."""
        self.log_info('∆ %r/%r %s (%rs)...',
                      signal.index,
                      self.case.total_signals,
                      signal.name.upper(),
                      timeout)

    async def on_signal_received(self,
                                 signal: BaseSignal,
                                 time_start: float,
                                 time_end: float) -> None:
        """Call when a signal related to this test is received."""
        latency = time_end - time_start
        self.signal_latency[signal.name] = latency

    async def on_failed(self, exc: BaseException) -> None:
        """Call when an invariant in the test has failed."""
        self.end()
        self.error = exc
        self.state = State.FAIL
        self.log.exception('Test failed: %r', exc)
        await self.case.on_test_failed(self, exc)
        await self._finalize_report()

    async def on_error(self, exc: BaseException) -> None:
        """Call when test execution raises error."""
        self.end()
        self.state = State.ERROR
        self.error = exc
        self.log.exception('Test raised: %r', exc)
        await self.case.on_test_error(self, exc)
        await self._finalize_report()

    async def on_timeout(self, exc: BaseException) -> None:
        """Call when test execution times out."""
        self.end()
        self.error = exc
        self.state = State.TIMEOUT
        self.log.exception('Test timed-out: %r', exc)
        await self.case.on_test_timeout(self, exc)
        await self._finalize_report()

    async def on_pass(self) -> None:
        """Call when test execution returns successfully."""
        self.end()
        self.error = None
        self.state = State.PASS
        human_secs = humanize_seconds(
            self.runtime or 0.0,
            microseconds=True,
            now='~0.0 seconds',
        )
        self.log_info('Test OK in %s √', human_secs)
        self._flush_logs()
        await self.case.on_test_pass(self)
        await self._finalize_report()

    async def _finalize_report(self) -> None:
        tb: Optional[str] = None
        error = self.error
        if error:
            tb = '\n'.join(traceback.format_tb(error.__traceback__))
        self.report = TestReport(
            case_name=self.case.name,
            state=self.state,
            test=self.test,
            runtime=self.runtime,
            signal_latency=self.signal_latency,
            error=str(error) if error else None,
            traceback=tb,
        )
        await self.case.post_report(self.report)

    def log_info(self, msg: str, *args: Any) -> None:
        """Log information related to the current execution."""
        if self.case.realtime_logs:
            self.log.info(msg, *args)
        else:
            self.logs.append((msg, args))

    def end(self) -> None:
        """End test execution."""
        self.ended = monotonic()
        self.runtime = self.ended - self.started

    def _flush_logs(self, severity: int = logging.INFO) -> None:
        logs = self.logs
        try:
            self.log.logger.log(severity, '\n'.join(
                self._format_log(severity, msg % log_args)  # noqa: S001
                for msg, log_args in logs))
        finally:
            logs.clear()
