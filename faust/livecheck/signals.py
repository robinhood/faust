"""LiveCheck - Signals and conditions tests can wait for."""
import abc
import asyncio
import typing
from time import monotonic
from typing import Any, Dict, Generic, Tuple, Type, TypeVar, cast
from mode import Seconds, want_seconds

from .exceptions import TestRaised, TestTimeout
from .locals import current_test_stack
from .models import SignalEvent
from .utils import to_model

if typing.TYPE_CHECKING:
    from .case import Case as _Case
else:
    class _Case: ...  # noqa

__all__ = ['BaseSignal', 'Signal']

KT = TypeVar('KT')
VT = TypeVar('VT')


class BaseSignal(Generic[KT, VT]):
    """Generic base class for signals."""

    name: str
    case: _Case
    index: int

    def __init__(self,
                 name: str = '',
                 case: _Case = None,
                 index: int = -1) -> None:
        self.name = name
        self.case = cast(_Case, case)
        self.index = index

    @abc.abstractmethod
    async def send(self, value: VT = None, *,
                   key: KT = None,
                   force: bool = False) -> None:
        ...

    @abc.abstractmethod
    async def wait(self, *,
                   key: KT = None,
                   timeout: Seconds = None) -> VT:
        ...

    async def resolve(self, key: KT, event: SignalEvent) -> None:
        self._set_current_value(key, event)
        self._wakeup_resolvers()

    def __set_name__(self, owner: Type, name: str) -> None:
        if not self.name:
            self.name = name

    def _wakeup_resolvers(self) -> None:
        self.case.app._can_resolve.set()

    async def _wait_for_resolved(self, *, timeout: float = None) -> None:
        app = self.case.app
        app._can_resolve.clear()
        await app.wait(app._can_resolve, timeout=timeout)

    def _get_current_value(self, key: KT) -> SignalEvent:
        return self.case.app._resolved_signals[self._index_key(key)]

    def _index_key(self, key: KT) -> Tuple[str, str, KT]:
        return self.name, self.case.name, key

    def _set_current_value(self, key: KT, event: SignalEvent) -> None:
        self.case.app._resolved_signals[self._index_key(key)] = event

    def clone(self, **kwargs: Any) -> 'BaseSignal':
        return type(self)(**{**self._asdict(), **kwargs})

    def _asdict(self, **kwargs: Any) -> Dict:
        return {'name': self.name, 'case': self.case, 'index': self.index}

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self.name}>'


class Signal(BaseSignal[KT, VT]):
    """Signal for test case using Kafka.

    Used to wait for something to happen elsewhere.

    """

    # What do we use for this? Kafka? some other mechanism?
    # I'm thinking separate Kafka cluster, with a single
    # topic for each test app.

    async def send(self, value: VT = None, *,
                   key: KT = None,
                   force: bool = False) -> None:
        current_test = current_test_stack.top
        if current_test is None:
            if not force:
                return
            assert key
        else:
            key = key if key is not None else current_test.id
        await self.case.app.bus.send(
            key=key,
            value=SignalEvent(
                signal_name=self.name,
                case_name=self.case.name,
                key=key,
                value=value,
            ),
        )

    async def wait(self, *,
                   key: KT = None,
                   timeout: Seconds = None) -> VT:
        # wait for key to arrive in consumer
        assert self.case.execution
        k: KT = cast(KT, self.case.execution.id) if key is None else key
        self.case.log.info(
            '∆ %r/%r %s (%rs)...',
            self.index,
            self.case.total_signals,
            self.name.upper(),
            timeout,
        )
        timeout_s = want_seconds(timeout)
        event = await self._wait_for_message_by_key(key=k, timeout=timeout_s)

        self._verify_event(event, k, self.name, self.case.name)
        return cast(VT, to_model(event.value))

    def _verify_event(self,
                      ev: SignalEvent,
                      key: KT,
                      name: str,
                      case: str) -> None:
        assert ev.key == key, f'{ev.key!r} == {key!r}'
        assert ev.signal_name == name, f'{ev.signal_name!r} == {name!r}'
        assert ev.case_name == case, f'{ev.case_name!r} == {case!r}'

    async def _wait_for_message_by_key(
            self, key: KT, *,
            timeout: float = None,
            max_interval: float = 2.0) -> SignalEvent:
        app = self.case.app
        time_start = monotonic()
        remaining = timeout
        # See if the key is already there.
        try:
            return self._get_current_value(key)
        except KeyError:
            pass
        # If not, wait for it to arrive.
        while not app.should_stop:
            if remaining is not None:
                remaining = remaining - (monotonic() - time_start)
            try:
                if remaining is not None and remaining <= 0.0:
                    try:
                        return self._get_current_value(key)
                    except KeyError:
                        raise asyncio.TimeoutError() from None
                max_wait = None
                if remaining is not None:
                    max_wait = min(remaining, max_interval)
                await self._wait_for_resolved(timeout=max_wait)
            except asyncio.TimeoutError:
                msg = f'Timed out waiting for signal {self.name} ({timeout})'
                raise TestTimeout(msg) from None
            if app.should_stop:
                raise asyncio.CancelledError()
            try:
                val = self._get_current_value(key)
                return val
            except KeyError:
                pass
        raise TestRaised('internal error')
