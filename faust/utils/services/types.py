import abc
import asyncio
from typing import (
    Any, AsyncContextManager, Awaitable, Callable,
    MutableMapping, Set, Type,
)
from ..times import Seconds
from ..types.collections import NodeT

__all__ = ['DiagT', 'ServiceT']


class DiagT(abc.ABC):
    """Diag keeps track of a services diagnostic flags."""
    flags: Set[str]
    last_transition: MutableMapping[str, float]

    @abc.abstractmethod
    def __init__(self, service: 'ServiceT') -> None:
        ...

    @abc.abstractmethod
    def set_flag(self, flag: str) -> None:
        ...

    @abc.abstractmethod
    def unset_flag(self, flag: str) -> None:
        ...


class ServiceT(AsyncContextManager):
    """Abstract type for an asynchronous service that can be started/stopped.

    See Also:
        :class:`faust.utils.services.Service`.
    """

    Diag: Type[DiagT]
    diag: DiagT

    shutdown_timeout: float
    wait_for_shutdown = False
    loop: asyncio.AbstractEventLoop = None
    restart_count: int = 0
    supervisor: 'SupervisorStrategyT' = None

    @abc.abstractmethod
    def __init__(self, *,
                 beacon: NodeT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    @abc.abstractmethod
    def add_dependency(self, service: 'ServiceT') -> 'ServiceT':
        ...

    @abc.abstractmethod
    async def start(self) -> None:
        ...

    @abc.abstractmethod
    async def maybe_start(self) -> None:
        ...

    @abc.abstractmethod
    async def crash(self, reason: BaseException) -> None:
        ...

    @abc.abstractmethod
    async def stop(self) -> None:
        ...

    @abc.abstractmethod
    async def restart(self) -> None:
        ...

    @abc.abstractmethod
    async def wait_until_stopped(self) -> None:
        ...

    @abc.abstractmethod
    def set_shutdown(self) -> None:
        ...

    @abc.abstractmethod
    def _repr_info(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def started(self) -> bool:
        ...

    @property
    @abc.abstractmethod
    def crashed(self) -> bool:
        ...

    @property
    @abc.abstractmethod
    def should_stop(self) -> bool:
        ...

    @property
    @abc.abstractmethod
    def state(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def label(self) -> str:
        ...

    @property
    @abc.abstractmethod
    def shortlabel(self) -> str:
        ...

    @property
    def beacon(self) -> NodeT:
        ...

    @beacon.setter
    def beacon(self, beacon: NodeT) -> None:
        ...


class SupervisorStrategyT(ServiceT):
    max_restarts: float
    over: float
    raises: Type[BaseException]

    @abc.abstractmethod
    def __init__(self,
                 *services: ServiceT,
                 max_restarts: Seconds = 100.0,
                 over: Seconds = 1.0,
                 raises: Type[BaseException] = None,
                 replacement: Callable[[ServiceT, int],
                                       Awaitable[ServiceT]] = None,
                 **kwargs: Any) -> None:
        self.replacement: Callable[[ServiceT, int], Awaitable[ServiceT]]

    @abc.abstractmethod
    def wakeup(self) -> None:
        ...

    @abc.abstractmethod
    def add(self, service: ServiceT) -> None:
        ...

    @abc.abstractmethod
    def discard(self, service: ServiceT) -> None:
        ...

    @abc.abstractmethod
    def service_operational(self, service: ServiceT) -> bool:
        ...

    @abc.abstractmethod
    async def restart_service(self, service: ServiceT) -> None:
        ...
