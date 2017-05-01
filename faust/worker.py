import asyncio
import faust
import logging
import os
import platform
import reprlib
import signal
import socket
import sys
from typing import Any, Coroutine, IO, Sequence, Set, Tuple, Union, cast
from progress.spinner import Spinner
from .types import AppT, SensorT
from .utils.compat import DummyContext
from .utils.logging import get_logger, level_name, setup_logging
from .utils.services import Service, ServiceT

try:  # pragma: no cover
    from setproctitle import setproctitle
except ImportError:  # pragma: no cover
    def setproctitle(title: str) -> None: ...  # noqa

__all__ = ['Worker']

PSIDENT = '[Faust:Worker]'

logger = get_logger(__name__)

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

F_IDENT = """
FAUST v{faust_v} {system} ({transport_v} {http_v} {py}={py_v})
""".strip()

F_BANNER = """
{art}
{ident}
[ .id          -> {id}
  .web         -> {web}
  .log         -> {logfile} ({loglevel})
  .pid         -> {pid}
  .hostname    -> {hostname}
  .transport   -> {transport} ]
""".strip()


class _TupleAsListRepr(reprlib.Repr):

    def repr_tuple(self, x: Tuple, level: int) -> str:
        return self.repr_list(cast(list, x), level)
_repr = _TupleAsListRepr().repr  # noqa: E305


class SpinnerHandler(logging.Handler):

    def __init__(self, worker: 'Worker', **kwargs: Any) -> None:
        self.worker = worker
        super().__init__(**kwargs)

    def emit(self, record: logging.LogRecord) -> None:
        if self.worker.spinner:
            self.worker.spinner.next()


class Worker(Service):
    art = ARTLINES
    f_ident = F_IDENT
    f_banner = F_BANNER

    debug: bool
    sensors: Set[SensorT]
    apps: Sequence[AppT]
    services: Sequence[ServiceT]
    loglevel: Union[str, int]
    logfile: Union[str, IO]
    stdout: IO
    stderr: IO
    quiet: bool

    def __init__(self, *services: ServiceT,
                 sensors: Sequence[SensorT] = None,
                 debug: bool = False,
                 quiet: bool = False,
                 loglevel: Union[str, int] = None,
                 logfile: Union[str, IO] = None,
                 logformat: str = None,
                 loop: asyncio.AbstractEventLoop = None,
                 stdout: IO = sys.stdout,
                 stderr: IO = sys.stderr,
                 **kwargs: Any) -> None:
        self.apps = [s for s in services if isinstance(s, AppT)]
        self.services = services
        self.sensors = set(sensors or [])
        self.debug = debug
        self.quiet = quiet
        self.loglevel = loglevel
        self.logfile = logfile
        self.logformat = logformat
        self.stdout = stdout
        self.stderr = stderr
        self.spinner = Spinner(file=self.stdout)
        super().__init__(loop=loop, **kwargs)
        for service in self.services:
            service.beacon.reattach(self.beacon)
            assert service.beacon.root is self.beacon

    def say(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._print(msg.format(*args, **kwargs))

    def carp(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._print(msg.format(*args, **kwargs), file=self.stderr)

    def _print(self, msg: str, file: IO = None, end: str = '\n') -> None:
        if not self.quiet:
            print(msg, file=file or self.stdout, end=end)

    def on_startup_finished(self) -> None:
        self.loop.call_later(3.0, self._on_startup_finished)

    def _on_startup_finished(self) -> None:
        if self.spinner:
            self.spinner.finish()
            self.spinner = None
            self.say('ready- ^')

    def faust_ident(self) -> str:
        return self.f_ident.format(
            py=platform.python_implementation(),
            faust_v=faust.__version__,
            system=platform.system(),
            transport_v=self.apps[0].transport.driver_version,
            http_v=self.apps[0].website.driver_version,
            py_v=platform.python_version(),
        )

    def print_banner(self) -> None:
        self.say(self.f_banner.format(
            art=self.art,
            ident=self.faust_ident(),
            id=', '.join(x.id for x in self.apps),
            transport=', '.join(x.url for x in self.apps),
            web=', '.join(x.website.url for x in self.apps),
            logfile=self.logfile if self.logfile else '-stderr-',
            loglevel=level_name(self.loglevel or 'WARN').lower(),
            pid=os.getpid(),
            hostname=socket.gethostname(),
        ))
        self._print('^ ', end='')

    def install_signal_handlers(self) -> None:
        self.loop.add_signal_handler(signal.SIGINT, self._on_sigint)
        self.loop.add_signal_handler(signal.SIGTERM, self._on_sigterm)

    def _on_sigint(self) -> None:
        self.carp('-INT- -INT- -INT- -INT- -INT- -INT-')
        self.add_future(self._stop_on_signal())

    def _on_sigterm(self) -> None:
        self.add_future(self._stop_on_signal())

    async def _stop_on_signal(self) -> None:
        logger.info('Worker: Stopping on signal received...')
        await self.stop()

    def execute_from_commandline(self, *coroutines: Coroutine) -> None:
        try:
            self.loop.run_until_complete(self.add_future(
                self._execute_from_commandline(*coroutines)))
        except asyncio.CancelledError:
            pass
        finally:
            self._shutdown_loop()

    def _shutdown_loop(self) -> None:
        try:
            self.loop.run_until_complete(self._gather_futures())
        except asyncio.CancelledError:
            pass
        self._gather_all()
        try:
            while self.loop.is_running():
                logger.info('Worker: Waiting for event loop to shutdown...')
                self.loop.stop()
                self.loop.run_until_complete(asyncio.sleep(1.0))
        finally:
            logger.info('Worker: Closing event loop')
            self.loop.close()

    def _gather_all(self) -> None:
        for i in range(40):
            if not asyncio.Task.all_tasks(loop=self.loop):
                break
            self.loop.run_until_complete(asyncio.sleep(0.1))

    async def _execute_from_commandline(self, *coroutines: Coroutine) -> None:
        setproctitle('[Faust:Worker] init')
        self.print_banner()
        with self._monitor():
            self.install_signal_handlers()
            await asyncio.gather(
                *[asyncio.ensure_future(coro, loop=self.loop)
                  for coro in coroutines],
                loop=self.loop)
            await self.start()
            self.add_future(self._stats())
            await self.wait_until_stopped()

    async def _stats(self) -> None:
        while not self.should_stop:
            await asyncio.sleep(5)
            if len(self.services) == 1:
                self.say(repr(self.services[0]))
            else:
                self.say(_repr(self.services))

    def _monitor(self) -> Any:
        if self.debug:
            try:
                import aiomonitor
            except ImportError:
                pass
            else:
                return aiomonitor.start_monitor(loop=self.loop)
        return DummyContext()

    def on_init_dependencies(self) -> Sequence[ServiceT]:
        for app in self.apps:
            for sensor in self.sensors:
                app.add_sensor(sensor)
        return self.services

    async def on_first_start(self) -> None:
        self._setup_logging()
        for sensor in self.sensors:
            await sensor.maybe_start()

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

    async def on_stop(self) -> None:
        for sensor in self.sensors:
            await sensor.stop()

    def _repr_info(self) -> str:
        return _repr(self.services)

    def _setproctitle(self, info: str) -> None:
        setproctitle('{} {}'.format(PSIDENT, info))
