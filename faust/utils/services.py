"""Async I/O services that can be started/stopped/shutdown."""
import abc
import asyncio
import logging
from types import TracebackType
from typing import (
    Any, Awaitable, Callable, ClassVar, Generator,
    Iterable, List, MutableSequence, Set, Type, Union, cast,
)
from .collections import Node
from .logging import CompositeLogger, get_logger
from .times import Seconds, want_seconds
from .types.collections import NodeT
from .types.services import ServiceT

__all__ = ['Service', 'ServiceBase', 'ServiceProxy']

__flake8_Set_is_used: Set  # XXX flake8 bug

FutureT = Union[asyncio.Future, Generator[Any, None, Any], Awaitable]

logger = get_logger(__name__)


class ServiceBase(ServiceT):
    """Base class for services."""
    log: CompositeLogger

    logger: logging.Logger = logger

    # This contains the common methods for Service and ServiceProxy

    def __init__(self) -> None:
        self.log = CompositeLogger(self)

    def _format_log(self, severity: int, msg: str,
                    *args: Any, **kwargs: Any) -> str:
        return f'[^{"-" * self.beacon.depth}{self.shortlabel}]: {msg}'

    def _log(self, severity: int, msg: str, *args: Any, **kwargs: Any) -> None:
        self.logger.log(severity, msg, *args, **kwargs)

    async def __aenter__(self) -> ServiceT:
        await self.start()
        return self

    async def __aexit__(self,
                        exc_type: Type[Exception],
                        exc_val: Exception,
                        exc_tb: TracebackType) -> None:
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


class ServiceTask:

    def __init__(self, fun: Callable[..., Awaitable]) -> None:
        self.fun: Callable[..., Awaitable] = fun

    async def __call__(self, obj: Any) -> Any:
        return await self.fun(obj)

    def __repr__(self) -> str:
        return repr(self.fun)


class Service(ServiceBase):
    """An asyncio service that can be started/stopped/restarted.

    Notes:
        Instantiating a service will create the asyncio event loop.

    Keyword Arguments:
        beacon (NodeT): Beacon used to track services in a graph.
        loop (asyncio.AbstractEventLoop): Event loop object.
    """

    #: Logger used by this service, subclasses should set their own.
    logger: logging.Logger = logger

    #: Set to True if .stop must wait for the shutdown flag to be set.
    wait_for_shutdown = False

    #: Time to wait for shutdown flag set before we give up.
    shutdown_timeout = 60.0

    #: Current number of times this service instance has been restarted.
    restart_count = 0

    _started: asyncio.Event
    _stopped: asyncio.Event
    _shutdown: asyncio.Event
    _crashed: asyncio.Event
    _crash_reason: Any

    #: The beacon is used to track the graph of services.
    _beacon: NodeT

    #: .add_dependency adds subservices to this list.
    #: They are started/stopped with the service.
    _children: MutableSequence[ServiceT]

    #: After child service is started it's added to this list,
    #: which is used by ``stop()`` to only stop services that have
    #: been actually started.
    _active_children: List[ServiceT]

    #: .add_future adds futures to this list
    #: They are started/stopped with the service.
    _futures: List[asyncio.Future]

    _tasks: ClassVar[List[ServiceTask]] = None

    @classmethod
    def task(cls, fun: Callable[..., Awaitable]) -> ServiceTask:
        """Decorator used to define a service background task.

        Example:
            >>> class S(Service):
            ...
            ... @Service.task
            ... async def background_task(self):
            ...     while not self.should_stop:
            ...         print('Waking up')
            ...         await self.sleep(1.0)
        """
        return ServiceTask(fun)

    def __init_subclass__(self) -> None:
        if self._tasks is None:
            self._tasks = []
        self._tasks.extend([
            value for key, value in self.__dict__.items()
            if isinstance(value, ServiceTask)
        ])

    def __init__(self, *,
                 beacon: NodeT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self._started = asyncio.Event(loop=self.loop)
        self._stopped = asyncio.Event(loop=self.loop)
        self._shutdown = asyncio.Event(loop=self.loop)
        self._crashed = asyncio.Event(loop=self.loop)
        self._crash_reason = None
        self._beacon = Node(self) if beacon is None else beacon.new(self)
        self._children = []
        self._active_children = []
        self._futures = []
        self.on_init()
        super().__init__()

    def add_dependency(self, service: ServiceT) -> ServiceT:
        """Add dependency to other service.

        The service will be started/stopped with this service.
        """
        if service.beacon is not None:
            service.beacon.reattach(self.beacon)
        self._children.append(service)
        return service

    def add_future(self, coro: Awaitable) -> asyncio.Future:
        """Add relationship to asyncio.Future.

        The future will be joined when this service is stopped.
        """
        fut = asyncio.ensure_future(self._execute_task(coro), loop=self.loop)
        self._futures.append(fut)
        return fut

    def on_init(self) -> None:
        """Callback to be called on instantiation."""
        ...

    def on_init_dependencies(self) -> Iterable[ServiceT]:
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

    async def sleep(self, n: Seconds) -> None:
        """Sleep for ``n`` seconds, or until service stopped."""
        try:
            await asyncio.wait_for(
                self._stopped.wait(), timeout=want_seconds(n), loop=self.loop)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    async def wait(self, *coros: FutureT, timeout: Seconds = None) -> None:
        """Wait for coroutines to complete, or until the service stops."""
        await self._wait_first(
            self._crashed.wait(),
            self._stopped.wait(),
            *coros,
            timeout=timeout,
        )

    async def _wait_first(
            self, *coros: FutureT, timeout: Seconds = None) -> None:
        await asyncio.wait(
            coros,
            timeout=want_seconds(timeout) if timeout is not None else None,
            return_when=asyncio.FIRST_COMPLETED,
            loop=self.loop,
        )

    async def start(self) -> None:
        """Start the service."""
        assert not self._started.is_set()
        self._started.set()
        if not self.restart_count:
            self._children.extend(self.on_init_dependencies())
            await self.on_first_start()
        self.log.info('Starting...')
        await self.on_start()
        for task in self._tasks:
            self.add_future(task(self))
        for child in self._children:
            if child is not None:
                await child.maybe_start()
                self._active_children.append(child)
        self.log.debug('Started.')
        await self.on_started()

    async def _execute_task(self, task: Awaitable) -> None:
        try:
            await task
        except asyncio.CancelledError:
            self.log.debug('Terminating cancelled task: %r', task)
        except Exception as exc:
            # the exception will be reraised by the main thread.
            await self.crash(exc)

    async def maybe_start(self) -> None:
        """Start the service, if it has not already been started."""
        if not self._started.is_set():
            await self.start()

    async def crash(self, reason: Exception) -> None:
        """Crash the service and all child services."""
        if not self._crashed.is_set():
            # We record the stack by raising the exception.
            self.log.exception('Crashed reason=%r', reason)

            root = self.beacon.root
            seen: Set[NodeT] = set()
            for node in self.beacon.walk():
                if node in seen:
                    self.log.warn(f'Recursive loop in beacon: {node}: {seen}')
                    if root and root.data is not self:
                        cast(Service, self.beacon.root.data)._crash(reason)
                    break
                seen.add(node)
                for child in [node.data] + node.children:
                    if isinstance(child, Service):
                        child._crash(reason)
            self._crash(reason)

    def _crash(self, reason: Exception) -> None:
        self._crashed.set()
        self._crash_reason = reason

    async def stop(self) -> None:
        """Stop the service."""
        if not self._stopped.is_set():
            self.log.info('Stopping...')
            self._stopped.set()
            await self.on_stop()
            for child in reversed(self._active_children):
                if child is not None:
                    await child.stop()
            self._active_children.clear()
            for future in reversed(self._futures):
                future.cancel()
            self.log.debug('-Stopped!')
            self.log.info('Shutting down...')
            if self.wait_for_shutdown:
                await asyncio.wait_for(
                    self._shutdown.wait(), self.shutdown_timeout,
                    loop=self.loop,
                )
            await self._gather_futures()
            await self.on_shutdown()
            self.log.debug('-Shutdown complete!')

    async def _gather_futures(self) -> None:
        while self._futures:
            # Gather all futures added via .add_future
            try:
                await asyncio.shield(asyncio.wait(
                    self._futures,
                    return_when=asyncio.ALL_COMPLETED,
                    loop=self.loop,
                ))
            except asyncio.CancelledError:
                continue
            else:
                break
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
        await self.wait()

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
    def shortlabel(self) -> str:
        """Label used for logging."""
        return self.label

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
        return type(self).__name__

    @property
    def shortlabel(self) -> str:
        return type(self).__name__

    @property
    def beacon(self) -> NodeT:
        return self._service.beacon

    @beacon.setter
    def beacon(self, beacon: NodeT) -> None:
        self._service.beacon = beacon
