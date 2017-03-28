"""Async I/O services that can be started/stopped/shutdown."""
import asyncio
from .logging import get_logger
from ..types.services import ServiceT

__all__ = ['Service']

logger = get_logger(__name__)


class Service(ServiceT):

    wait_for_shutdown = False
    shutdown_timeout = 60.0

    _started: asyncio.Event
    _stopped: asyncio.Event
    _shutdown: asyncio.Event

    def __init__(self, *, loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self._started = asyncio.Event(loop=self.loop)
        self._stopped = asyncio.Event(loop=self.loop)
        self._shutdown = asyncio.Event(loop=self.loop)
        self.on_init()

    async def __aenter__(self) -> 'Service':
        await self.start()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.stop()

    def on_init(self) -> None:
        ...

    async def on_start(self) -> None:
        ...

    async def on_stop(self) -> None:
        ...

    async def on_shutdown(self) -> None:
        ...

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
                await asyncio.wait_for(  # type: ignore
                    self._shutdown.wait(), self.shutdown_timeout,
                    loop=self.loop,
                )
            await self.on_shutdown()
            logger.info('-Shutdown service %r', self)

    def set_shutdown(self) -> None:
        self._shutdown.set()

    def __repr__(self) -> str:
        return '<{name}: {self.state}>'.format(
            name=type(self).__name__, self=self)

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
