"""Async I/O services that can be started/stopped/shutdown."""
import abc
import asyncio
import weakref
from typing import (
    Any, Awaitable, List, MutableSequence, Sequence, cast,
)
from .collections import Node
from .logging import get_logger
from .types.collections import NodeT
from .types.services import ServiceT

__all__ = ['Service', 'ServiceBase', 'ServiceProxy', 'ServiceT']

logger = get_logger(__name__)


class ServiceBase(ServiceT):
    """Base class for services."""

    # This contains the common methods for Service and ServiceProxy

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
    """An asyncio service that can be started/stopped/restarted.

    Notes:
        Instantiating a service will create the asyncio event loop.

    Keyword Arguments:
        beacon (NodeT): Beacon used to track services in a graph.
        loop (asyncio.AbstractEventLoop): Event loop object.
    """

    #: Set to True if .stop must wait for the shutdown flag to be set.
    wait_for_shutdown = False

    #: Time to wait for shutdown flag set before we give up.
    shutdown_timeout = 60.0

    #: Current number of times this service instance has been restarted.
    restart_count = 0

    _started: asyncio.Event
    _stopped: asyncio.Event
    _shutdown: asyncio.Event

    #: The becon is used to track the graph of services.
    _beacon: NodeT

    #: .add_dependency adds subservices to this list.
    #: They are started/stopped with the service.
    _children: MutableSequence[Any]

    #: .add_future adds futures to this list
    #: They are started/stopped with the service.
    _futures: List[asyncio.Future]

    def __init__(self, *,
                 beacon: NodeT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self._started = asyncio.Event(loop=self.loop)
        self._stopped = asyncio.Event(loop=self.loop)
        self._shutdown = asyncio.Event(loop=self.loop)
        self._beacon = Node(self) if beacon is None else beacon.new(self)
        self._children = []
        self._futures = []
        self.on_init()

    def add_dependency(self, service: ServiceT) -> ServiceT:
        """Add dependency to other service.

        The service will be started/stopped with this service.
        """
        if service.beacon.root is None:
            service.beacon.reattach(self.beacon)
        self._children.append(weakref.ref(service))
        return service

    def add_future(self, coro: Awaitable) -> asyncio.Future:
        """Add relationship to asyncio.Future.

        The future will be joined when this service is stopped.
        """
        fut = asyncio.ensure_future(coro, loop=self.loop)
        self._futures.append(fut)
        return fut

    def on_init(self) -> None:
        """Callback to be called on instantiation."""
        ...

    def on_init_dependencies(self) -> Sequence[ServiceT]:
        """Callback to be used to add service dependencies."""
        return []

    async def on_first_start(self) -> None:
        """Callback to be called the first time the service is started."""
        ...

    async def on_start(self) -> None:
        """Callback to be called every time the service is started."""
        ...

    async def on_started(self) -> None:
        """Callback to be called once the service is started/restarted."""
        ...

    async def on_stop(self) -> None:
        """Callback to be called when the service is signalled to stop."""
        ...

    async def on_shutdown(self) -> None:
        """Callback to be called when the service is shut down."""
        ...

    async def on_restart(self) -> None:
        """Callback to be called when the service is restarted."""
        ...

    async def start(self) -> None:
        """Start the service."""
        logger.info('+Starting service %r', self)
        assert not self._started.is_set()
        self._started.set()
        if not self.restart_count:
            self._children.extend(
                map(weakref.ref, self.on_init_dependencies()))
            await self.on_first_start()
        await self.on_start()
        for childref in self._children:
            child = childref()
            if child is not None:
                await cast(ServiceT, child).maybe_start()
        logger.info('-Started service %r', self)
        await self.on_started()

    async def maybe_start(self) -> None:
        """Start the service, if it has not already been started."""
        if not self._started.is_set():
            await self.start()

    async def stop(self) -> None:
        """Stop the service."""
        if not self._stopped.is_set():
            logger.info('+Stopping service %r', self)
            self._stopped.set()
            await self.on_stop()
            for childref in reversed(self._children):
                child = childref()
                if child is not None:
                    await cast(ServiceT, child).stop()
            for future in reversed(self._futures):
                future.cancel()
            logger.info('-Stopped service %r', self)
            logger.info('+Shutdown service %r', self)
            if self.wait_for_shutdown:
                await asyncio.wait_for(
                    self._shutdown.wait(), self.shutdown_timeout,
                    loop=self.loop,
                )
            await self._gather_futures()
            await self.on_shutdown()
            logger.info('-Shutdown service %r', self)

    async def _gather_futures(self) -> None:
        # Gather all futures added via .add_future
        try:
            await asyncio.gather(*self._futures, loop=self.loop)
        except asyncio.CancelledError:
            pass
        finally:
            self._futures.clear()

    async def restart(self) -> None:
        """Restart this service."""
        self.restart_count += 1
        await self.stop()
        for ev in (self._started, self._stopped, self._shutdown):
            ev.clear()
        self.on_init()
        await self.start()

    async def wait_until_stopped(self) -> None:
        """Wait until the service is signalled to stop."""
        await self._stopped.wait()

    def set_shutdown(self) -> None:
        """Set the shutdown signal.

        Notes:
            If :attr:`wait_for_shutdown` is set, stopping the service
            will wait for this flag to be set.
        """
        self._shutdown.set()

    @property
    def started(self) -> bool:
        """Was the service started?"""
        return self._started.is_set()

    @property
    def should_stop(self) -> bool:
        """Should the service stop ASAP?"""
        return self._stopped.is_set()

    @property
    def state(self) -> str:
        """Current service state - as a human readable string."""
        if not self._started.is_set():
            return 'init'
        if not self._stopped.is_set():
            return 'running'
        if not self._shutdown.is_set():
            return 'stopping'
        return 'shutdown'

    @property
    def label(self) -> str:
        """Label used for graphs."""
        return type(self).__name__

    @property
    def beacon(self) -> NodeT:
        """Beacon used to track services in a dependency graph."""
        return self._beacon

    @beacon.setter
    def beacon(self, beacon: NodeT) -> None:
        self._beacon = beacon


class ServiceProxy(ServiceBase):
    """A service proxy delegates ServiceT methods to a composite service.

    Example:

        >>> class MyServiceProxy(ServiceProxy):
        ...
        ...     @cached_property
        ...     def _service(self) -> ServiceT:
        ...         return ActualService()

    Notes:
        Since the Faust App is created at module-level, it must use a service
        proxy to ensure the event loop is not also created at that time.
    """

    @property
    @abc.abstractmethod
    def _service(self) -> ServiceT:
        ...

    def add_dependency(self, service: ServiceT) -> ServiceT:
        return self._service.add_dependency(service)

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
