"""ƒAµS† Worker

A worker starts one or more Faust applications.
"""
import asyncio
import logging
import os
import platform
import reprlib
import signal
import socket
import sys

from contextlib import suppress
from itertools import chain
from pathlib import Path
from typing import (
    Any, Coroutine, IO, Iterable, Set, Tuple, Type, Union, cast,
)

from progress.spinner import Spinner

from . import __version__ as faust_version
from .types import AppT, SensorT
from .utils.compat import DummyContext
from .utils.debug import BlockingDetector
from .utils.imports import SymbolArg, symbol_by_name
from .utils.logging import cry, get_logger, level_name, setup_logging
from .utils.objects import cached_property
from .utils.services import Service, ServiceT
from .web.site import Website as _Website

try:  # pragma: no cover
    from setproctitle import setproctitle
except ImportError:  # pragma: no cover
    def setproctitle(title: str) -> None: ...  # noqa

__all__ = ['DEBUG', 'DEFAULT_BLOCKING_TIMEOUT', 'Worker']

#: Path to default Web site class.
DEFAULT_WEBSITE_CLS = 'faust.web.site:Website'

#: Name prefix of process in ps/top listings.
PSIDENT = '[Faust:Worker]'

#: Enables debugging features (like blockdetection).
DEBUG: bool = bool(os.environ.get('F_DEBUG', False))

#: Blocking detection timeout
DEFAULT_BLOCKING_TIMEOUT: float = float(os.environ.get(
    'F_BLOCKING_TIMEOUT', 10.0,
))

#: ASCII-art used in startup banner.
ARTLINES = """\
                                       .x+=:.        s
   oec :                               z`    ^%      :8
  @88888                 x.    .          .   <k    .88
  8"*88%        u      .@88k  z88u      .@8Ned8"   :888ooo
  8b.        us888u.  ~"8888 ^8888    .@^%8888"  -*8888888
 u888888> .@88 "8888"   8888  888R   x88:  `)8b.   8888
  8888R   9888  9888    8888  888R   8888N=*8888   8888
  8888P   9888  9888    8888  888R    %8"    R88   8888
  *888>   9888  9888    8888 ,888B .   @8Wou 9%   .8888Lu=
  4888    9888  9888   "8888Y 8888"  .888888P`    ^%888*
  '888    "888*""888"   `Y"   'YP    `   ^"F        'Y"
   88R     ^Y"   ^Y'
   88>
   48
   '8
"""

#: Format string for startup banner.
F_BANNER = """
{art}
{ident}
[ .id          -> {app.id}
  .web         -> {website.url}
  .log         -> {logfile} ({loglevel})
  .pid         -> {pid}
  .hostname    -> {hostname}
  .transport   -> {app.url}
  .store       -> {app.store} ]
""".strip()

#: Format string for banner info line.
F_IDENT = """
ƒaµS† v{faust_v} {system} ({transport_v} {http_v} {py}={py_v})
""".strip()

logger = get_logger(__name__)


class _TupleAsListRepr(reprlib.Repr):

    def repr_tuple(self, x: Tuple, level: int) -> str:
        return self.repr_list(cast(list, x), level)
# this repr formats tuples as if they are lists.
_repr = _TupleAsListRepr().repr  # noqa: E305


class SpinnerHandler(logging.Handler):
    """A logger handler that iterates our progress spinner for each log."""

    def __init__(self, worker: 'Worker', **kwargs: Any) -> None:
        self.worker = worker
        super().__init__(**kwargs)

    def emit(self, record: logging.LogRecord) -> None:
        if self.worker.spinner:
            self.worker.spinner.next()  # noqa: B305


class Worker(Service):
    """Worker.

    Arguments:
        *services (ServiceT): Services to start with worker.
            This includes application instances to start.

    Keyword Arguments:
        sensors (Iterable[SensorT]): List of sensors to include.
        debug (bool): Enables debugging mode [disabled by default].
        quiet (bool): Do not output anything to console [disabled by default].
        loglevel (Union[str, int]): Level to use for logging, can be string
            (one of: CRIT|ERROR|WARN|INFO|DEBUG), or integer.
        logfile (Union[str, IO]): Name of file or a stream to log to.
        logformat (str): Format to use when logging messages.
        stdout (IO): Standard out stream.
        stderr (IO): Standard err stream.
        loop (asyncio.AbstractEventLoop): Custom event loop object.
    """
    logger = logger

    art = ARTLINES
    f_ident = F_IDENT
    f_banner = F_BANNER

    app: AppT
    debug: bool
    sensors: Set[SensorT]
    services: Iterable[ServiceT]
    loglevel: Union[str, int]
    logfile: Union[str, IO]
    logformat: str
    stdout: IO
    stderr: IO
    blocking_timeout: float
    workdir: str
    web_port: int
    web_bind: str
    Website: Type[_Website]
    spinner: Spinner
    quiet: bool

    def __init__(
            self, app: AppT, *services: ServiceT,
            sensors: Iterable[SensorT] = None,
            debug: bool = DEBUG,
            quiet: bool = False,
            loglevel: Union[str, int] = None,
            logfile: Union[str, IO] = None,
            logformat: str = None,
            stdout: IO = sys.stdout,
            stderr: IO = sys.stderr,
            blocking_timeout: float = DEFAULT_BLOCKING_TIMEOUT,
            workdir: str = None,
            Website: SymbolArg = DEFAULT_WEBSITE_CLS,
            web_port: int = None,
            web_bind: str = None,
            loop: asyncio.AbstractEventLoop = None,
            **kwargs: Any) -> None:
        self.app = app
        self.services = services
        self.sensors = set(sensors or [])
        self.debug = debug
        self.quiet = quiet
        self.loglevel = loglevel
        self.logfile = logfile
        self.logformat = logformat
        self.stdout = stdout
        self.stderr = stderr
        self.blocking_timeout = blocking_timeout
        self.workdir = workdir
        self.Website = symbol_by_name(Website)
        self.web_port = web_port
        self.web_bind = web_bind
        super().__init__(loop=loop, **kwargs)

        self.spinner = Spinner(file=self.stdout)
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

    async def on_startup_finished(self) -> None:
        self.add_future(self._on_startup_finished())

    async def _on_startup_finished(self) -> None:
        if self.debug:
            await self._blockdetect.maybe_start()
        if self.spinner:
            self.spinner.finish()
            self.spinner = None
            self.say('ready- ^')
        else:
            self.log.info('Ready')

    def faust_ident(self) -> str:
        return self.f_ident.format(
            py=platform.python_implementation(),
            faust_v=faust_version,
            system=platform.system(),
            transport_v=self.app.transport.driver_version,
            http_v=self.website.web.driver_version,
            py_v=platform.python_version(),
        )

    def print_banner(self) -> None:
        self.say(self.f_banner.format(
            art=self.art,
            ident=self.faust_ident(),
            app=self.app,
            website=self.website.web,
            logfile=self.logfile if self.logfile else '-stderr-',
            loglevel=level_name(self.loglevel or 'WARN').lower(),
            pid=os.getpid(),
            hostname=socket.gethostname(),
        ))
        self._say('^ ', end='')

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
        finally:
            # Then close the loop.
            self.log.info('Closing event loop')
            self.loop.close()
        if self._crash_reason:
            self.log.crit(
                'We experienced a crash! Reraising original exception...')
            raise self._crash_reason from self._crash_reason

    def _gather_all(self) -> None:
        # sleeps for at most 40 * 0.1s
        for _ in range(40):
            if not asyncio.Task.all_tasks(loop=self.loop):
                break
            self.loop.run_until_complete(asyncio.sleep(0.1))

    async def _execute_from_commandline(self, *coroutines: Coroutine) -> None:
        setproctitle('[Faust:Worker] init')
        self.print_banner()
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

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        # App beacon is now a child of worker.
        self.app.beacon.reattach(self.beacon)
        for sensor in self.sensors:
            self.app.sensors.add(sensor)
        self.app.on_startup_finished = self.on_startup_finished
        return chain([self.website], self.services, [self.app])

    async def on_first_start(self) -> None:
        if self.workdir:
            os.chdir(Path(self.workdir).absolute())
        self._setup_logging()

    def _setup_logging(self) -> None:
        _loglevel: int = None
        if self.loglevel:
            _loglevel = setup_logging(
                loglevel=self.loglevel,
                logfile=self.logfile,
                logformat=self.logformat,
            )
        if _loglevel and _loglevel < logging.WARN:
            self.spinner = None

        if self.spinner:
            logging.root.handlers[0].setLevel(_loglevel)
            logging.root.addHandler(
                SpinnerHandler(self, level=logging.DEBUG))
            logging.root.setLevel(logging.DEBUG)

    def _repr_info(self) -> str:
        return _repr(self.services)

    def _setproctitle(self, info: str) -> None:
        setproctitle(f'{PSIDENT} {info}')

    @cached_property
    def _blockdetect(self) -> BlockingDetector:
        return BlockingDetector(
            self.blocking_timeout,
            beacon=self.beacon,
            loop=self.loop,
        )

    @cached_property
    def website(self) -> _Website:
        return self.Website(
            self.app,
            bind=self.web_bind,
            port=self.web_port,
            loop=self.loop,
            beacon=self.beacon,
        )
