import abc
from datetime import timedelta
from random import uniform
from typing import Any, ClassVar, Dict, List, Tuple, Type, cast
from mode import Seconds, want_seconds

__all__ = ['Signal', 'Case']


class Signal(abc.ABC):
    """Signal for test case.

    Used to wait for something to happen elsewhere.

    """

    # What do we use for this? Kafka? some other mechanism?
    # I'm thinking separate Kafka cluster, with a single
    # topic for each test app.

    def __init__(self,
                 name: str = None,
                 case: 'Case' = None) -> None:
        self.name: str = name
        self.case: 'Case' = case

    def __set_name__(self, owner: Type, name: str) -> None:
        if not self.name:
            self.name = name

    async def send(self, key: Any, value: Any) -> None:
        await self.case.topic.send(key=(self.name, key), value=value)

    async def wait(self, key: Any, *, timeout: Seconds) -> Any:
        timeout_s = want_seconds(timeout)
        # wait for key to arrive in consumer
        return await self._wait_for_message_by_key(
            key=(self.name, key), timeout=timeout_s)

    async def _wait_for_message_by_key(self, key: Any,
                                       timeout: float = None) -> Any:
        raise NotImplementedError()

    def clone(self, **kwargs: Any) -> 'Signal':
        return type(self)(**{**self._asdict(), **kwargs})

    def _asdict(self, **kwargs: Any) -> Dict:
        return {'name': self.name, 'case': self.case}

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self.name}>'


class Case(abc.ABC):
    """Livecheck test case."""

    warn_empty_after: Seconds = timedelta(minutes=30)
    probability: float = 0.2
    active: bool = False
    test_args: Tuple
    test_kwargs: Dict

    Signal: Type[Signal] = Signal
    _signals: ClassVar[List[Signal]] = cast(List[Signal], None)

    signals: List[Signal] = cast(List[Signal], None)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        # mypy does not recognize `__init_subclass__` as a classmethod
        # and thinks we're mutating a ClassVar when setting:
        #   cls.some_classvar_attr = False
        # To fix this we always delegate to a _init_subclass classmethod.
        cls._init_subclass(**kwargs)

    @classmethod
    def _init_subclass(cls, **kwargs):
        # Find any attributes of type Signal and gather them
        # in the self._signals list for introspection purposes.
        #
        # We need a list of signals to start a consumer for them.
        if cls._signals is None:
            cls._signals = []
        for value in cls.__dict__.values():
            if isinstance(value, cls.Signal):
                cls._signals.append(value)

    @abc.abstractmethod
    async def run(self, *test_args: Any, **test_kwargs: Any) -> None:
        ...  # Subclasses must implement run

    def __init__(self, *args: Any, **kwargs: Any):
        self.active = uniform(0, 1) < self.probability
        self.test_args = args
        self.test_kwargs = kwargs
        self.signals = [sig.clone(case=self) for sig in self._signals]
