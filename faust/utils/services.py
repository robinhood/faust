"""Async I/O services that can be started/stopped/shutdown."""
import abc
import asyncio
from typing import Any
from .collections import Node
from .logging import get_logger
from .types.collections import NodeT
from .types.services import ServiceT

__all__ = ['Service', 'ServiceT']

logger = get_logger(__name__)


class ServiceBase(ServiceT):

    wait_for_shutdown = False
    shutdown_timeout = 60.0
    restart_count = 0

    def on_init(self) -> None:
        ...

    async def on_start(self) -> None:
        ...

    async def on_stop(self) -> None:
        ...

    async def on_shutdown(self) -> None:
        ...

    async def on_restart(self) -> None:
        ...

    async def __aenter__(self) -> ServiceT:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.stop()

    def __repr__(self) -> str:
        info = self._repr_info()
        return '<{name}: {self.state}{info}>'.format(
            name=type(self).__name__,
            self=self,
            info=' ' + info if info else '',
        )

    def _repr_info(self) -> str:
        return ''


class Service(ServiceBase):

    _started: asyncio.Event
    _stopped: asyncio.Event
    _shutdown: asyncio.Event
    _beacon: NodeT

    def __init__(self, *,
                 beacon: NodeT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self._started = asyncio.Event(loop=self.loop)
        self._stopped = asyncio.Event(loop=self.loop)
        self._shutdown = asyncio.Event(loop=self.loop)
        self._beacon = Node(self) if beacon is None else beacon.new(self)
        self.on_init()

    async def start(self) -> None:
        logger.info('+Starting service %r', self)
        assert not self._started.is_set()
        self._started.set()
        await self.on_start()
        logger.info('-Started service %r', self)

    async def maybe_start(self) -> None:
        if not self._started.is_set():
            await self.start()

    async def stop(self) -> None:
        if not self._stopped.is_set():
            logger.info('+Stopping service %r', self)
            self._stopped.set()
            await self.on_stop()
            logger.info('-Stopped service %r', self)
            logger.info('+Shutdown service %r', self)
            if self.wait_for_shutdown:
                await asyncio.wait_for(
                    self._shutdown.wait(), self.shutdown_timeout,
                    loop=self.loop,
                )
            await self.on_shutdown()
            logger.info('-Shutdown service %r', self)

    async def restart(self) -> None:
        self.restart_count += 1
        await self.stop()
        for ev in (self._started, self._stopped, self._shutdown):
            ev.clear()
        self.on_init()
        await self.start()

    async def wait_until_stopped(self) -> None:
        await self._stopped.wait()

    def set_shutdown(self) -> None:
        self._shutdown.set()

    @property
    def started(self) -> bool:
        return self._started.is_set()

    @property
    def should_stop(self) -> bool:
        return self._stopped.is_set()

    @property
    def state(self) -> str:
        if not self._started.is_set():
            return 'init'
        if not self._stopped.is_set():
            return 'running'
        if not self._shutdown.is_set():
            return 'stopping'
        return 'shutdown'

    @property
    def label(self) -> str:
        return type(self).__name__

    @property
    def beacon(self) -> NodeT:
        return self._beacon

    @beacon.setter
    def beacon(self, beacon: NodeT) -> None:
        self._beacon = beacon


class ServiceProxy(ServiceBase):

    @property
    @abc.abstractmethod
    def _service(self) -> ServiceT:
        ...

    async def start(self) -> None:
        await self._service.start()

    async def maybe_start(self) -> None:
        await self._service.maybe_start()

    async def stop(self) -> None:
        await self._service.stop()

    async def restart(self) -> None:
        await self._service.restart()

    async def wait_until_stopped(self) -> None:
        await self._service.wait_until_stopped()

    def set_shutdown(self) -> None:
        self._service.set_shutdown()

    @property
    def started(self) -> bool:
        return self._service.started

    @property
    def should_stop(self) -> bool:
        return self._service.should_stop

    @property
    def state(self) -> str:
        return self._service.state

    @property
    def label(self) -> str:
        return self._service.label

    @property
    def beacon(self) -> NodeT:
        return self._service.beacon

    @beacon.setter
    def beacon(self, beacon: NodeT) -> None:
        self._service.beacon = beacon
