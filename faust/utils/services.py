"""Async I/O services that can be started/stopped/shutdown."""
import abc
import asyncio
import logging
import os
import reprlib
import signal
import sys
import threading
import traceback
import typing
from contextlib import suppress
from functools import wraps
from time import monotonic
from types import TracebackType
from typing import (
    Any, Awaitable, Callable, ClassVar, Coroutine, Generator, IO,
    Iterable, List, MutableSequence, Sequence, Set, Tuple, Type, Union, cast,
)
from .collections import Node
from .compat import DummyContext
from .logging import CompositeLogger, cry, get_logger, setup_logging
from .objects import cached_property
from .times import Seconds, want_seconds
from .types.collections import NodeT
from .types.services import DiagT, ServiceT

if typing.TYPE_CHECKING:
    from .debug import BlockingDetector
else:
    class BlockingDetector: ...  # noqa

__all__ = [
    'Service',
    'ServiceBase',
    'ServiceProxy',
    'ServiceThread',
    'ServiceWorker',
    'Diag',
]

FutureT = Union[asyncio.Future, Generator[Any, None, Any], Awaitable]

logger = get_logger(__name__)


class _TupleAsListRepr(reprlib.Repr):

    def repr_tuple(self, x: Tuple, level: int) -> str:
        return self.repr_list(cast(list, x), level)
# this repr formats tuples as if they are lists.
_repr = _TupleAsListRepr().repr  # noqa: E305


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


class Diag(DiagT):

    def __init__(self, service: ServiceT) -> None:
        self.service = service
        self.flags = set()
        self.last_transition = {}

    def set_flag(self, flag: str) -> None:
        self.flags.add(flag)
        self.last_transition[flag] = monotonic()

    def unset_flag(self, flag: str) -> None:
        self.flags.discard(flag)


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
    Diag: Type[DiagT] = Diag

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
    _crash_reason: Exception

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

    #: The ``@Service.task`` decorator adds :class:`ServiceTask`
    #: instances to this list (which is a class variable).
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

    @classmethod
    def transitions_to(cls, flag: str) -> Callable:
        def _decorate(
                fun: Callable[..., Awaitable]) -> Callable[..., Awaitable]:
            @wraps(fun)
            async def _and_transition(self: ServiceT,
                                      *args: Any, **kwargs: Any) -> Any:
                self.diag.set_flag(flag)
                try:
                    return await fun(self, *args, **kwargs)
                finally:
                    self.diag.unset_flag(flag)
            return _and_transition
        return _decorate

    def __init_subclass__(self) -> None:
        # Every new subclass adds @Service.task decorated methods
        # to the class-local `_tasks` list.
        if self._tasks is None:
            self._tasks = []
        self._tasks.extend([
            value for key, value in self.__dict__.items()
            if isinstance(value, ServiceTask)
        ])

    def __init__(self, *,
                 beacon: NodeT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.diag = self.Diag(self)
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

    async def transition_with(self, flag: str, fut: Awaitable,
                              *args: Any, **kwargs: Any) -> Any:
        self.diag.set_flag(flag)
        try:
            return await fut
        finally:
            self.diag.unset_flag(flag)

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

    async def join_services(self, services: Sequence[ServiceT]) -> None:
        for service in services:
            try:
                await service.maybe_start()
            except Exception as exc:
                await self.crash(exc)
        for service in reversed(services):
            await service.stop()

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
        except RuntimeError as exc:
            if 'Event loop is closed' in str(exc):
                self.log.info('Cancelled task %r: %s', task, exc)
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
            self.log.debug('Shutting down...')
            if self.wait_for_shutdown:
                self.log.debug('Waiting for shutdown')
                await asyncio.wait_for(
                    self._shutdown.wait(), self.shutdown_timeout,
                    loop=self.loop,
                )
                self.log.debug('Shutting down now')
            for future in reversed(self._futures):
                future.cancel()
            await self._gather_futures()
            await self.on_shutdown()
            self.log.info('-Stopped!')

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
        for ev in (self._started, self._stopped, self._shutdown, self._crashed):
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
        if self._crashed.is_set():
            return 'crashed'
        elif not self._started.is_set():
            return 'init'
        elif not self._stopped.is_set():
            return 'running'
        elif not self._shutdown.is_set():
            return 'stopping'
        else:
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


class ServiceThread(threading.Thread):
    _shutdown: threading.Event
    _stopped: threading.Event

    def __init__(self, service: ServiceT,
                 *,
                 daemon: bool = False,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.service = service
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self._loop = loop
        super().__init__(daemon=daemon)

    def run(self) -> None:
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._serve(self._loop))

    async def _start_service(self, loop: asyncio.AbstractEventLoop) -> None:
        service = self.service
        service.loop = loop
        await service.start()
        await self.on_start()

    async def on_start(self) -> None:
        ...

    async def _stop_service(self) -> None:
        await self.service.stop()
        await self.on_stop()

    async def on_stop(self) -> None:
        ...

    async def _serve(self, loop: asyncio.AbstractEventLoop) -> None:
        shutdown_set = self._shutdown.is_set
        await self._start_service(loop)
        try:
            while not shutdown_set():
                try:
                    await asyncio.sleep(1, loop=loop)
                except Exception as exc:  # pylint: disable=broad-except
                    try:
                        self.on_crash('{0!r} crashed: {1!r}', self.name, exc)
                        self._set_stopped()
                    finally:
                        os._exit(1)  # exiting by normal means won't work
        finally:
            try:
                await self._stop_service()
            finally:
                self._set_stopped()

    def on_crash(self, msg: str, *fmt: Any, **kwargs: Any) -> None:
        print(msg.format(*fmt), file=sys.stderr)
        traceback.print_exc(None, sys.stderr)

    def _set_stopped(self) -> None:
        try:
            self._stopped.set()
        except TypeError:
            # we lost the race at interpreter shutdown,
            # so gc collected built-in modules.
            pass

    def stop(self) -> None:
        """Graceful shutdown."""
        self._shutdown.set()
        self._stopped.wait()
        if self.is_alive():
            self.join(threading.TIMEOUT_MAX)


class ServiceWorker(Service):

    stdout: IO
    stderr: IO
    debug: bool
    quiet: bool
    blocking_timeout: float
    loglevel: Union[str, int]
    logfile: Union[str, IO]
    logformat: str

    services: Iterable[ServiceT]

    def __init__(
            self, *services: ServiceT,
            debug: bool = False,
            quiet: bool = False,
            loglevel: Union[str, int] = None,
            logfile: Union[str, IO] = None,
            logformat: str = None,
            stdout: IO = sys.stdout,
            stderr: IO = sys.stderr,
            blocking_timeout: float = None,
            loop: asyncio.AbstractEventLoop = None,
            **kwargs: Any) -> None:
        self.services = services
        self.debug = debug
        self.quiet = quiet
        self.loglevel = loglevel
        self.logfile = logfile
        self.logformat = logformat
        self.stdout = stdout
        self.stderr = stderr
        self.blocking_timeout = blocking_timeout
        super().__init__(loop=loop, **kwargs)

        for service in self.services:
            service.beacon.reattach(self.beacon)
            assert service.beacon.root is self.beacon

    def say(self, msg: str) -> None:
        """Write message to standard out."""
        self._say(msg)

    def carp(self, msg: str) -> None:
        """Write warning to standard err."""
        self._say(msg, file=self.stderr)

    def _say(self, msg: str, file: IO = None, end: str = '\n') -> None:
        if not self.quiet:
            print(msg, file=file or self.stdout, end=end)  # noqa: T003

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        return self.services

    async def on_first_start(self) -> None:
        self._setup_logging()

    def _setup_logging(self) -> None:
        _loglevel: int = None
        if self.loglevel:
            _loglevel = setup_logging(
                loglevel=self.loglevel,
                logfile=self.logfile,
                logformat=self.logformat,
            )
        self.on_setup_root_logger(logging.root, _loglevel)

    def on_setup_root_logger(self,
                             logger: logging.Logger,
                             level: int) -> None:
        ...

    async def maybe_start_blockdetection(self) -> None:
        if self.debug:
            await self._blockdetect.maybe_start()

    def install_signal_handlers(self) -> None:
        self.loop.add_signal_handler(signal.SIGINT, self._on_sigint)
        self.loop.add_signal_handler(signal.SIGTERM, self._on_sigterm)
        self.loop.add_signal_handler(signal.SIGUSR1, self._on_sigusr1)

    def _on_sigint(self) -> None:
        self.carp('-INT- -INT- -INT- -INT- -INT- -INT-')
        asyncio.ensure_future(self._stop_on_signal(), loop=self.loop)

    def _on_sigterm(self) -> None:
        asyncio.ensure_future(self._stop_on_signal(), loop=self.loop)

    def _on_sigusr1(self) -> None:
        self.add_future(self._cry())

    async def _cry(self) -> None:
        cry(file=self.stderr)

    async def _stop_on_signal(self) -> None:
        self.log.info('Stopping on signal received...')
        await self.stop()

    def execute_from_commandline(self, *coroutines: Coroutine) -> None:
        try:
            with suppress(asyncio.CancelledError):
                self.loop.run_until_complete(self.add_future(
                    self._execute_from_commandline(*coroutines)))
        finally:
            if not self._stopped.is_set():
                self.loop.run_until_complete(self.stop())
            self._shutdown_loop()

    def _shutdown_loop(self) -> None:
        # Gather futures created by us.
        self.log.info('Gathering service tasks...')
        with suppress(asyncio.CancelledError):
            self.loop.run_until_complete(self._gather_futures())
        # Gather absolutely all asyncio futures.
        self.log.info('Gathering all futures...')
        self._gather_all()
        try:
            # Wait until loop is fully stopped.
            while self.loop.is_running():
                self.log.info('Waiting for event loop to shutdown...')
                self.loop.stop()
                self.loop.run_until_complete(asyncio.sleep(1.0))
        except Exception as exc:
            self.log.exception('Got exception while waiting: %r', exc)
        finally:
            # Then close the loop.
            fut = asyncio.ensure_future(self._sentinel_task(), loop=self.loop)
            self.loop.run_until_complete(fut)
            self.loop.stop()
            self.log.info('Closing event loop')
            self.loop.close()
            if self._crash_reason:
                self.log.crit(
                    'We experienced a crash! Reraising original exception...')
                raise self._crash_reason from self._crash_reason

    async def _sentinel_task(self) -> None:
        await asyncio.sleep(1.0, loop=self.loop)

    def _gather_all(self) -> None:
        # sleeps for at most 40 * 0.1s
        for _ in range(40):
            if not len(asyncio.Task.all_tasks(loop=self.loop)):
                break
            self.loop.run_until_complete(asyncio.sleep(0.1))
        for task in asyncio.Task.all_tasks(loop=self.loop):
            task.cancel()

    async def on_execute(self) -> None:
        ...

    async def _execute_from_commandline(self, *coroutines: Coroutine) -> None:
        await self.on_execute()
        with self._monitor():
            self.install_signal_handlers()
            if coroutines:
                await asyncio.wait(
                    [asyncio.ensure_future(coro, loop=self.loop)
                     for coro in coroutines],
                    loop=self.loop,
                    return_when=asyncio.ALL_COMPLETED,
                )
            await self.start()
            await self.wait_until_stopped()

    def _monitor(self) -> Any:
        if self.debug:
            with suppress(ImportError):
                import aiomonitor
                return aiomonitor.start_monitor(loop=self.loop)
        return DummyContext()

    def _repr_info(self) -> str:
        return _repr(self.services)

    @cached_property
    def _blockdetect(self) -> BlockingDetector:
        from . import debug
        return debug.BlockingDetector(
            self.blocking_timeout,
            beacon=self.beacon,
            loop=self.loop,
        )


__flake8_Set_is_used: Set  # XXX flake8 bug
