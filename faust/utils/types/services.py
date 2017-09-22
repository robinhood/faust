import abc
import asyncio
from types import TracebackType
from typing import AsyncContextManager, MutableMapping, Set, Type
from .collections import NodeT

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
