"""Worker.

A "worker" starts a single instance of a Faust application.

See Also:
    :ref:`app-starting`: for more information.
"""
import asyncio
import logging
import os
import sys
from collections import defaultdict
from itertools import chain
from pathlib import Path
from typing import Any, Dict, IO, Iterable, Mapping, Optional, Set, Union

import mode
from mode import ServiceT, get_logger
from mode.utils.logging import formatter

from .types import AppT, SensorT, TP, TopicT
from .types._env import BLOCKING_TIMEOUT, CONSOLE_PORT, DEBUG
from .utils import terminal

try:  # pragma: no cover
    # if installed we use this to set ps.name (argv[0])
    from setproctitle import setproctitle
except ImportError:  # pragma: no cover
    def setproctitle(title: str) -> None: ...  # noqa

__all__ = ['Worker']

#: Name prefix of process in ps/top listings.
PSIDENT = '[Faust:Worker]'

logger = get_logger(__name__)


@formatter
def format_log_arguments(arg: Any) -> Any:  # pragma: no cover
    if arg and isinstance(arg, Mapping):
        first_k, first_v = next(iter(arg.items()))
        if (isinstance(first_k, str) and isinstance(first_v, set) and
                isinstance(next(iter(first_v), None), TopicT)):
            return '\n' + terminal.logtable(
                [(k, v) for k, v in arg.items()],
                title='Subscription',
                headers=['Topic', 'Descriptions'],
            )
        elif isinstance(first_v, TP):
            return '\n' + terminal.logtable(
                [(k.topic, k.partition, v) for k, v in arg.items()],
                title='Topic Partition Map',
                headers=['topic', 'partition', 'offset'],
            )
    elif arg and isinstance(arg, (set, list)):
        if isinstance(next(iter(arg)), TP):
            topics: Dict[str, Set[int]] = defaultdict(set)
            for tp in arg:
                topics[tp.topic].add(tp.partition)

            return '\n' + terminal.logtable(
                [(k, repr(v)) for k, v in topics.items()],
                title='Topic Partition Set',
                headers=['topic', 'partitions'],
            )


class Worker(mode.Worker):
    """Worker.

    Usage:
        You can start a worker using:

            1) the :program:`faust worker` program.

            2) instantiating Worker programmatically and calling
               `execute_from_commandline()`::

                    >>> worker = Worker(app)
                    >>> worker.execute_from_commandline()

            3) or if you already have an event loop, calling ``await start``,
               but in that case *you are responsible for gracefully shutting
               down the event loop*::

                    async def start_worker(worker: Worker) -> None:
                        await worker.start()

                    def manage_loop():
                        loop = asyncio.get_event_loop()
                        worker = Worker(app, loop=loop)
                        try:
                            loop.run_until_complete(start_worker(worker)
                        finally:
                            worker.stop_and_shutdown_loop()

    Arguments:
        app: The Faust app to start.
        *services: Services to start with worker.
            This includes application instances to start.

        sensors (Iterable[SensorT]): List of sensors to include.
        debug (bool): Enables debugging mode [disabled by default].
        quiet (bool): Do not output anything to console [disabled by default].
        loglevel (Union[str, int]): Level to use for logging, can be string
            (one of: CRIT|ERROR|WARN|INFO|DEBUG), or integer.
        logfile (Union[str, IO]): Name of file or a stream to log to.
        stdout (IO): Standard out stream.
        stderr (IO): Standard err stream.
        blocking_timeout (float): When :attr:`debug` is enabled this
            sets the timeout for detecting that the event loop is blocked.
        workdir (Union[str, Path]): Custom working directory for the process
            that the worker will change into when started.
            This working directory change is permanent for the process,
            or until something else changes the working directory again.
        loop (asyncio.AbstractEventLoop): Custom event loop object.
    """

    logger = logger

    #: The Faust app started by this worker.
    app: AppT

    #: Additional sensors to add to the Faust app.
    sensors: Set[SensorT]

    #: Current working directory.
    #: Note that if passed as an argument to Worker, the worker
    #: will change to this directory when started.
    workdir: Path

    #: Class that displays a terminal progress spinner (see :pypi:`progress`).
    spinner: Optional[terminal.Spinner]

    #: Set by signal to avoid printing an OK status.
    _shutdown_immediately: bool = False

    def __init__(self,
                 app: AppT,
                 *services: ServiceT,
                 sensors: Iterable[SensorT] = None,
                 debug: bool = DEBUG,
                 quiet: bool = False,
                 loglevel: Union[str, int] = None,
                 logfile: Union[str, IO] = None,
                 stdout: IO = sys.stdout,
                 stderr: IO = sys.stderr,
                 blocking_timeout: float = BLOCKING_TIMEOUT,
                 workdir: Union[Path, str] = None,
                 console_port: int = CONSOLE_PORT,
                 loop: asyncio.AbstractEventLoop = None,
                 redirect_stdouts: bool = None,
                 redirect_stdouts_level: int = None,
                 logging_config: Dict = None,
                 **kwargs: Any) -> None:
        self.app = app
        self.sensors = set(sensors or [])
        self.workdir = Path(workdir or Path.cwd())
        if redirect_stdouts is None:
            redirect_stdouts = app.conf.worker_redirect_stdouts
        if redirect_stdouts_level is None:
            redirect_stdouts_level = app.conf.worker_redirect_stdouts_level
        if logging_config is None:
            logging_config = app.conf.logging_config
        super().__init__(
            *services,
            debug=debug,
            quiet=quiet,
            loglevel=loglevel,
            logfile=logfile,
            loghandlers=app.conf.loghandlers,
            stdout=stdout,
            stderr=stderr,
            blocking_timeout=blocking_timeout,
            console_port=console_port,
            redirect_stdouts=redirect_stdouts,
            redirect_stdouts_level=redirect_stdouts_level,
            logging_config=logging_config,
            loop=loop,
            **kwargs)
        self.spinner = terminal.Spinner(file=self.stdout)

    async def on_start(self) -> None:
        self.app.in_worker = True
        await super().on_start()

    def _on_sigint(self) -> None:
        self._flag_as_shutdown_by_signal()
        super()._on_sigint()

    def _on_sigterm(self) -> None:
        self._flag_as_shutdown_by_signal()
        super()._on_sigterm()

    def _flag_as_shutdown_by_signal(self) -> None:
        self._shutdown_immediately = True
        if self.spinner:
            self.spinner.stop()

    async def on_startup_finished(self) -> None:
        if self._shutdown_immediately:
            return self._on_shutdown_immediately()
        # block detection started here after changelog stuff,
        # and blocking RocksDB bulk updates.
        await self.maybe_start_blockdetection()
        self._on_startup_end_spinner()

    def _on_startup_end_spinner(self) -> None:
        if self.spinner:
            self.spinner.finish()
            if self.spinner.file.isatty():
                self.say(' 😊')
            else:
                self.say(' OK ^')
        else:
            self.log.info('Ready')

    def _on_shutdown_immediately(self) -> None:
        self.say('')  # make sure spinner newlines.

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        # App service is now a child of worker.
        self.app.beacon.reattach(self.beacon)
        # Transfer sensors to app
        for sensor in self.sensors:
            self.app.sensors.add(sensor)
        # Callback called once the app is running and fully
        # functional, we use it to e.g. print the "ready" message.
        self.app.on_startup_finished = self.on_startup_finished
        return chain(self.services, [self.app])

    async def on_first_start(self) -> None:
        self.change_workdir(self.workdir)
        self.autodiscover()
        await self.default_on_first_start()

    def change_workdir(self, path: Path) -> None:
        if path and path.absolute() != path.cwd().absolute():
            os.chdir(path.absolute())

    def autodiscover(self) -> None:
        if self.app.conf.autodiscover:
            self.app.discover()

    def _setproctitle(self, info: str, *, ident: str = PSIDENT) -> None:
        setproctitle(f'{ident} -{info}- {self._proc_ident()}')

    def _proc_ident(self) -> str:
        conf = self.app.conf
        return f'{conf.id} {self._proc_web_ident()} {conf.datadir.absolute()}'

    def _proc_web_ident(self) -> str:
        conf = self.app.conf
        if conf.web_transport.scheme == 'unix':
            return f'{conf.web_transport}'
        return f'-p {conf.web_port}'

    async def on_execute(self) -> None:
        # This is called as soon as we start
        self._setproctitle('init')
        if self.spinner and self.spinner.file.isatty():
            self._say('starting➢ ', end='', flush=True)

    def on_worker_shutdown(self) -> None:
        # This is called when we start the shutdown process.
        self._setproctitle('stopping')
        if self.spinner and self.spinner.file.isatty():
            self.spinner.reset()
            self._say('stopping➢ ', end='', flush=True)

    def on_setup_root_logger(self, logger: logging.Logger, level: int) -> None:
        # This is used to set up the terminal progress spinner
        # so that it spins for every log message emitted.
        self._disable_spinner_if_level_below_WARN(level)
        self._setup_spinner_handler(logger, level)

    def _disable_spinner_if_level_below_WARN(self, level: int) -> None:
        if level and level < logging.WARN:
            self.spinner = None

    def _setup_spinner_handler(
            self, logger: logging.Logger, level: int) -> None:
        if self.spinner:
            logger.handlers[0].setLevel(level)
            logger.addHandler(
                terminal.SpinnerHandler(self.spinner, level=logging.DEBUG))
            logger.setLevel(logging.DEBUG)
