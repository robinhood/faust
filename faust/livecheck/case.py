"""LiveCheck - Test cases."""
import abc
import typing
from random import uniform
from time import time
from typing import Any, Dict, Iterable, Mapping, Optional
from mode import Seconds, want_seconds
from mode.utils.logging import CompositeLogger

from .exceptions import LiveCheckError, TestFailed, TestRaised
from .locals import current_test_stack
from .models import SignalEvent, TestExecution
from .signals import BaseSignal
from .utils import to_model

if typing.TYPE_CHECKING:
    from .app import LiveCheck as _LiveCheck
else:
    class _LiveCheck: ...  # noqa

__all__ = ['Case']


class Case(abc.ABC):
    """LiveCheck test case."""

    app: _LiveCheck

    #: Name of the test
    #: If not set this will be generated out of the subclass name.
    name: str

    #: Timeout in seconds for when after we warn that nothing is processing.
    warn_empty_after: float = 1800.0

    #: Probability of test running when live traffic is going through.
    probability: float = 0.2

    #: Is test activated (based on probability setting).
    active: bool = False

    signals: Dict[str, BaseSignal]
    _original_signals: Iterable[BaseSignal]

    total_signals: int

    log: CompositeLogger

    def __init__(self, *,
                 app: _LiveCheck,
                 name: str,
                 probability: float = None,
                 warn_empty_after: Seconds = None,
                 active: bool = None,
                 signals: Iterable[BaseSignal] = None,
                 execution: TestExecution = None) -> None:
        self.app = app
        self.name = name
        if probability is not None:
            self.probability = probability
        if warn_empty_after is not None:
            self.warn_empty_after = want_seconds(warn_empty_after)
        if active is not None:
            self.active = active
        self.execution = execution
        self._original_signals = signals or ()
        self.signals = {
            sig.name: sig.clone(case=self)
            for sig in self._original_signals
        }

        self.total_signals = len(self.signals)
        # update local attribute so that the
        # signal attributes have the correct signal instance.
        self.__dict__.update(self.signals)

        self.log = CompositeLogger(self.app.log, formatter=self._format_log)

    def clone(self, **kwargs: Any) -> 'Case':
        return type(self)(**{**self._asdict(), **kwargs})

    def _asdict(self) -> Mapping[str, Any]:
        return {
            'app': self.app,
            'name': self.name,
            'probability': self.probability,
            'warn_empty_after': self.warn_empty_after,
            'active': self.active,
            'signals': self._original_signals,
            'execution': self.execution,
        }

    async def maybe_trigger(self, id: str,
                            *args: Any,
                            **kwargs: Any) -> Optional[TestExecution]:
        if uniform(0, 1) < self.probability:
            return await self.trigger(id, *args, **kwargs)
        return None

    async def trigger(self, id: str,
                      *args: Any,
                      **kwargs: Any) -> TestExecution:
        execution = TestExecution(
            id=id,
            case_name=self.name,
            timestamp=time(),
            test_args=args,
            test_kwargs=kwargs,
        )
        await self.app.pending_tests.send(key=id, value=execution)
        return execution

    @abc.abstractmethod
    async def run(self, *test_args: Any, **test_kwargs: Any) -> None:
        ...  # Subclasses must implement run

    async def resolve_signal(self, key: str, event: SignalEvent) -> None:
        await self.signals[event.signal_name].resolve(key, event)

    async def execute(self, test: TestExecution) -> None:
        # resolve_models
        runner = self.clone(execution=test)
        with current_test_stack.push(test):
            args = [self._prepare_arg(arg) for arg in test.test_args]
            kwargs = {
                self._prepare_kwkey(k): self._prepare_kwval(v)
                for k, v in test.test_kwargs.items()
            }
            runner.log.info('≈≈≈ Test %s executing... (issued %s) ≈≈≈',
                            self.name, test.human_date)
            try:
                await runner.run(*args, **kwargs)
            except AssertionError as exc:
                runner.log.exception('Test failed: %r', exc)
                raise TestFailed(exc) from exc
            except LiveCheckError as exc:
                runner.log.exception('Test raised: %r', exc)
                raise
            except Exception as exc:
                runner.log.exception('Test raised: %r', exc)
                raise TestRaised(exc) from exc
            else:
                runner.log.info('Test OK √')

    def _prepare_arg(self, arg: Any) -> Any:
        return to_model(arg)

    def _prepare_kwkey(self, arg: Any) -> Any:
        return to_model(arg)

    def _prepare_kwval(self, val: Any) -> Any:
        return to_model(val)

    def _format_log(self, severity: int, msg: str,
                    *args: Any, **kwargs: Any) -> str:
        if self.execution:
            return f'[{self.execution.shortident}] {msg}'
        return f'[{self.name}] {msg}'
