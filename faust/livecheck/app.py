"""LiveCheck - Faust Application."""
from datetime import timedelta
from typing import Any, Callable, ClassVar, Dict, Iterable, Tuple, Type

import faust
from faust.types import ChannelT, StreamT, TopicT
from mode.utils.objects import annotations, cached_property, qualname
from mode.utils.times import Seconds

from .case import Case
from .exceptions import LiveCheckError
from .models import SignalEvent, TestExecution
from .signals import BaseSignal, Signal

__all__ = ['LiveCheck']

#: alias for mypy bug
_Case = Case


class LiveCheck(faust.App):
    """LiveCheck application."""

    Signal: ClassVar[Type[BaseSignal]]
    Signal = Signal

    Case: ClassVar[Type[Case]]  # type: ignore
    Case = _Case

    test_topic_name: str
    bus_topic_name: str

    cases: Dict[str, _Case]
    bus: ChannelT

    _resolved_signals: Dict[Tuple[str, Any], SignalEvent]

    def __init__(self,
                 id: str,
                 *,
                 test_topic_name: str = 'livecheck',
                 bus_topic_name: str = 'livecheck-bus',
                 **kwargs: Any) -> None:
        super().__init__(id, **kwargs)
        self.test_topic_name = test_topic_name
        self.bus_topic_name = bus_topic_name
        self.cases = {}
        self._resolved_signals = {}

    def case(self, *,
             name: str = None,
             probability: float = 0.2,
             warn_empty_after: Seconds = timedelta(minutes=30),
             active: bool = False,
             base: Type[_Case] = Case) -> Callable[[Type], _Case]:
        base_case = base

        def _inner(cls: Type) -> _Case:
            case_cls = type(cls.__name__, (cls, base_case), {
                '__module__': cls.__module__,
                'app': self,
            })
            fields, defaults = annotations(
                case_cls,
                stop=base_case,
                skip_classvar=True,
                localns={case_cls.__name__: case_cls},
            )
            signals = []

            def find_signals() -> Iterable[str]:
                for attr_name, attr_type in fields.items():
                    actual_type = getattr(attr_type, '__origin__', attr_type)
                    if issubclass(actual_type, BaseSignal):
                        yield attr_name

            for i, attr_name in enumerate(find_signals()):
                signal = getattr(case_cls, attr_name, None)
                if signal is None:
                    signal = self.Signal(name=attr_name, index=i + 1)
                    setattr(case_cls, attr_name, signal)
                    signals.append(signal)
                else:
                    signal.index = i + 1

            return self.add_case(case_cls(
                app=self,
                name=name or qualname(cls),
                probability=probability,
                warn_empty_after=warn_empty_after,
                signals=signals,
            ))
        return _inner

    def add_case(self, case: _Case) -> _Case:
        self.cases[case.name] = case
        return case

    async def on_start(self) -> None:
        self.agent(self.bus)(self._populate_signals)
        self.agent(self.pending_tests)(self._execute_tests)

    async def _populate_signals(self, events: StreamT[SignalEvent]) -> None:
        async for test_id, event in events.items():
            event.case_name = self._prepare_case_name(event.case_name)
            case = self.cases[event.case_name]
            await case.resolve_signal(test_id, event)

    async def _execute_tests(self, tests: StreamT[TestExecution]) -> None:
        async for test_id, test in tests.items():
            test.case_name = self._prepare_case_name(test.case_name)
            case = self.cases[test.case_name]
            try:
                await case.execute(test)
            except LiveCheckError:
                pass

    def _prepare_case_name(self, name: str) -> str:
        if name.startswith('__main__.'):
            if not self.conf.origin:
                raise RuntimeError('LiveCheck app missing origin argument')
            return self.conf.origin + name[8:]
        return name

    @cached_property
    def bus(self) -> TopicT:
        return self.topic(
            self.bus_topic_name,
            key_type=str,
            value_type=SignalEvent,
        )

    @cached_property
    def pending_tests(self) -> TopicT:
        return self.topic(
            self.test_topic_name,
            key_type=str,
            value_type=TestExecution,
        )
