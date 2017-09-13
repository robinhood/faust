"""Worker

A worker starts a Faust application, and includes support for logging
signal-handling, etc.
"""
import asyncio
import logging
import os
import platform
import socket
import sys

from itertools import chain
from pathlib import Path
from typing import Any, IO, Iterable, Set, Type, Union

from progress.spinner import Spinner

from . import __version__ as faust_version
from .bin._env import DEBUG, DEFAULT_BLOCKING_TIMEOUT
from .types import AppT, SensorT
from .utils.imports import SymbolArg, symbol_by_name
from .utils.logging import get_logger, level_name
from .utils.objects import cached_property
from .utils.services import ServiceT, ServiceWorker
from .web.site import Website as _Website

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(title: str) -> None: ...  # noqa

__all__ = ['Worker']

#: Path to default Web site class.
DEFAULT_WEBSITE_CLS = 'faust.web.site:Website'

#: Name prefix of process in ps/top listings.
PSIDENT = '[Faust:Worker]'

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
  .loop        -> {loop}
  .transport   -> {app.url} {transport_extra}
  .store       -> {app.store} ]
""".strip()

#: Format string for banner info line.
F_IDENT = """
ƒaµS† v{faust_v} {system} ({transport_v} {http_v} {py}={py_v})
""".strip()

logger = get_logger(__name__)


class SpinnerHandler(logging.Handler):
    """A logger handler that iterates our progress spinner for each log."""

    def __init__(self, worker: 'Worker', **kwargs: Any) -> None:
        self.worker = worker
        super().__init__(**kwargs)

    def emit(self, record: logging.LogRecord) -> None:
        if self.worker.spinner:
            self.worker.spinner.next()  # noqa: B305


class Worker(ServiceWorker):
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
    sensors: Set[SensorT]
    workdir: Path
    web_port: int
    web_bind: str
    Website: Type[_Website]
    spinner: Spinner

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
            workdir: Union[Path, str] = None,
            Website: SymbolArg[Type[_Website]] = DEFAULT_WEBSITE_CLS,
            web_port: int = None,
            web_bind: str = None,
            with_uvloop: bool = False,
            advertised_host: str = None,
            loop: asyncio.AbstractEventLoop = None,
            **kwargs: Any) -> None:
        self.app = app
        self.sensors = set(sensors or [])
        self.workdir = Path(workdir or Path.cwd())
        self.Website = symbol_by_name(Website)
        self.web_port = web_port
        self.web_bind = web_bind
        super().__init__(
            *services,
            debug=debug,
            quiet=quiet,
            loglevel=loglevel,
            logfile=logfile,
            logformat=logformat,
            stdout=stdout,
            stderr=stderr,
            blocking_timeout=blocking_timeout,
            loop=loop,
            **kwargs)
        self.spinner = Spinner(file=self.stdout)

    async def on_startup_finished(self) -> None:
        self.add_future(self._on_startup_finished())

    async def _on_startup_finished(self) -> None:
        await self.maybe_start_blockdetection()
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
        transport_extra = ''
        if self.loop.__class__.__module__ == 'uvloop':
            transport_extra = '+uvloop'
        self.say(self.f_banner.format(
            art=self.art,
            ident=self.faust_ident(),
            app=self.app,
            website=self.website.web,
            logfile=self.logfile if self.logfile else '-stderr-',
            loglevel=level_name(self.loglevel or 'WARN').lower(),
            pid=os.getpid(),
            hostname=socket.gethostname(),
            transport_extra=transport_extra,
            loop=asyncio.get_event_loop(),
        ))
        self._say('^ ', end='')

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
        await super().on_first_start()  # <-- sets up logging

    def _setproctitle(self, info: str) -> None:
        setproctitle(f'{PSIDENT} {info}')

    async def on_execute(self) -> None:
        self._setproctitle('init')
        self.print_banner()

    def on_setup_root_logger(self,
                             logger: logging.Logger,
                             level: int) -> None:
        if level and level < logging.WARN:
            self.spinner = None

        if self.spinner:
            logger.handlers[0].setLevel(level)
            logger.addHandler(
                SpinnerHandler(self, level=logging.DEBUG))
            logger.setLevel(logging.DEBUG)

    @cached_property
    def website(self) -> _Website:
        return self.Website(
            self.app,
            bind=self.web_bind,
            port=self.web_port,
            loop=self.loop,
            beacon=self.beacon,
        )
