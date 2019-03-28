import abc
from datetime import timedelta
from random import uniform
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    List,
    Type,
    TypeVar,
    cast,
)
from mode import Seconds, want_seconds
from mode.utils.objects import annotations, cached_property, qualname
from faust import App, Record
from faust.types import ChannelT, StreamT, TopicT

__all__ = ['Signal', 'Case', 'Livecheck']

KT = TypeVar('KT')
VT = TypeVar('VT')


class SignalEvent(Record):
    signal_name: str
    case_name: str
    key: Any
    value: Any


class TestExecution(Record):
    id: str
    case_name: str
    timestamp: float
    test_args: List[Any]
    test_kwargs: Dict[str, Any]


class BaseSignal(Generic[KT, VT]):
    resolved: Dict[str, SignalEvent]

    def __init__(self,
                 name: str = '',
                 case: 'BaseCase' = None) -> None:
        self.name: str = name
        self.case: 'BaseCase' = cast('BaseCase', case)
        self.resolved = {}

    @abc.abstractmethod
    async def send(self, key: KT, value: VT) -> None:
        ...

    @abc.abstractmethod
    async def wait(self, key: KT, *, timeout: Seconds) -> VT:
        ...

    async def resolve(self, key: str, event: SignalEvent) -> None:
        self.resolved[key] = event

    def __set_name__(self, owner: Type, name: str) -> None:
        if not self.name:
            self.name = name

    def clone(self, **kwargs: Any) -> 'BaseSignal':
        return type(self)(**{**self._asdict(), **kwargs})

    def _asdict(self, **kwargs: Any) -> Dict:
        return {'name': self.name, 'case': self.case}

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self.name}>'


class Signal(BaseSignal[KT, VT]):
    """Signal for test case.

    Used to wait for something to happen elsewhere.

    """

    # What do we use for this? Kafka? some other mechanism?
    # I'm thinking separate Kafka cluster, with a single
    # topic for each test app.

    async def send(self, key: KT, value: VT) -> None:
        await self.case.app.bus.send(key=key, value=value)

    async def wait(self, key: KT, *, timeout: Seconds) -> VT:
        timeout_s = want_seconds(timeout)
        # wait for key to arrive in consumer
        return await self._wait_for_message_by_key(key=key, timeout=timeout_s)

    async def _wait_for_message_by_key(self, key: KT,
                                       timeout: float = None) -> VT:
        raise NotImplementedError()


class BaseCase(abc.ABC):
    """Livecheck test case."""

    app: 'Livecheck'

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

    def __init__(self, *,
                 app: 'Livecheck',
                 name: str,
                 probability: float = None,
                 warn_empty_after: Seconds = None,
                 active: bool = None,
                 signals: Iterable[BaseSignal] = None) -> None:
        self.name = name
        if probability is not None:
            self.probability = probability
        if warn_empty_after is not None:
            self.warn_empty_after = warn_empty_after
        if active is None:
            active = uniform(0, 1) < self.probability
        self.active = active
        self.signals = {
            sig.name: sig.clone(case=self)
            for sig in signals or ()
        }

    @abc.abstractmethod
    async def run(self, *test_args: Any, **test_kwargs: Any) -> None:
        ...  # Subclasses must implement run

    async def resolve_signal(self, key: str, event: SignalEvent) -> None:
        self.signals[event.signal_name].resolve(key, event)

    async def execute(self, test: TestExecution) -> None:
        await self.run(*test.test_args, **test.test_kwargs)

    @classmethod
    def prepare_signals(
            cls, signals: Iterable[BaseSignal]) -> Dict[str, BaseSignal]:
        return {sig.name: sig for sig in signals}


class Case(BaseCase):
    ...


class Livecheck(App):
    Case: ClassVar[Type[BaseCase]] = Case
    Signal: ClassVar[Type[BaseSignal]] = Signal

    test_topic_name: str
    bus_topic_name: str

    cases: Dict[str, BaseCase]
    bus: ChannelT

    def __init__(self,
                 id: str = 'livecheck',
                 test_topic_name: str = 'livecheck',
                 bus_topic_name: str = 'livecheck-bus',
                 **kwargs: Any) -> None:
        super().__init__(id, **kwargs)
        self.test_topic_name = test_topic_name
        self.bus_topic_name = bus_topic_name
        self.cases = {}

    def case(self, *,
             name: str = None,
             probability: float = 0.2,
             warn_empty_after: Seconds = timedelta(minutes=30),
             active: bool = False,
             base: Type[BaseCase] = None) -> Callable[[Type], BaseCase]:
        base_case = base or self.Case

        def _inner(cls: Type) -> BaseCase:
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
            for attr_name, attr_type in fields.items():
                actual_type = getattr(attr_type, '__origin__', attr_type)
                if issubclass(actual_type, BaseSignal):
                    signal = getattr(case_cls, attr_name, None)
                    if signal is None:
                        signal = self.Signal(name=attr_name)
                    setattr(case_cls, attr_name, signal)
                    signals.append(signal)

            return self.add_case(case_cls(
                app=self,
                name=name or qualname(cls),
                probability=probability,
                warn_empty_after=warn_empty_after,
                signals=signals,
            ))
        return _inner

    def add_case(self, case: BaseCase) -> BaseCase:
        self.cases[case.name] = case
        return case

    async def on_start(self) -> None:
        self.agent(self.bus)(self._populate_signals)
        self.agent(self.pending_tests(self._execute_tests))

    async def _populate_signals(self, events: StreamT[SignalEvent]) -> None:
        async for test_id, event in events.items():
            await self.cases[event.case_name].resolve_signal(test_id, event)

    async def _execute_tests(self, tests: StreamT[TestExecution]) -> None:
        async for test_id, test in tests.items():
            await self.cases[test.case_name].execute(test)

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
