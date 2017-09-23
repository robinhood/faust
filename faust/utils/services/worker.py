import asyncio
import logging
import reprlib
import signal
import sys
import typing
from contextlib import suppress
from typing import Any, IO, Iterable, Tuple, Union, cast
from .services import Service
from .types import ServiceT
from ..compat import DummyContext
from ..logging import cry, get_logger, setup_logging
from ..objects import cached_property

if typing.TYPE_CHECKING:
    from .debug import BlockingDetector
else:
    class BlockingDetector: ...  # noqa

__all__ = ['ServiceWorker']

logger = get_logger(__name__)


class _TupleAsListRepr(reprlib.Repr):

    def repr_tuple(self, x: Tuple, level: int) -> str:
        return self.repr_list(cast(list, x), level)
# this repr formats tuples as if they are lists.
_repr = _TupleAsListRepr().repr  # noqa: E305


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
        await self.on_execute()
        with self._monitor():
            self.install_signal_handlers()

    async def on_execute(self) -> None:
        ...

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

    def execute_from_commandline(self) -> None:
        try:
            with suppress(asyncio.CancelledError):
                self.loop.run_until_complete(self.start())
        finally:
            self.stop_and_shutdown()

    def stop_and_shutdown(self) -> None:
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
        except BaseException as exc:
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

    async def start(self) -> None:
        await super().start()
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
