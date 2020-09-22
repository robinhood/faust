"""Program ``faust worker`` used to start application from console."""
import asyncio
import os
import platform
import socket
from typing import Any, List, Optional, Tuple, Type, cast

from mode import ServiceT, Worker
from mode.utils.imports import symbol_by_name
from mode.utils.logging import level_name
from yarl import URL

from faust.worker import Worker as FaustWorker
from faust.types import AppT
from faust.types._env import WEB_BIND, WEB_PORT, WEB_TRANSPORT
from faust.utils.terminal.tables import TableDataT

from . import params
from .base import AppCommand, now_builtin_worker_options, option

__all__ = ['worker']

FAUST = 'ƒaµS†'

# XXX mypy borks if we do `from faust import __version`.
faust_version: str = symbol_by_name('faust:__version__')


class worker(AppCommand):
    """Start worker instance for given app."""

    daemon = True
    redirect_stdouts = True

    worker_options = [
        option('--with-web/--without-web',
               default=True,
               help='Enable/disable web server and related components.'),
        option('--web-port', '-p',
               default=None, type=params.TCPPort(),
               help=f'Port to run web server on (default: {WEB_PORT})'),
        option('--web-transport',
               default=None, type=params.URLParam(),
               help=f'Web server transport (default: {WEB_TRANSPORT})'),
        option('--web-bind', '-b', type=str),
        option('--web-host', '-h',
               default=socket.gethostname(), type=str,
               help=f'Canonical host name for the web server '
                    f'(default: {WEB_BIND})'),
    ]

    options = (cast(List, worker_options) +
               cast(List, now_builtin_worker_options))

    def on_worker_created(self, worker: Worker) -> None:
        """Print banner when worker starts."""
        self.say(self.banner(worker))

    def as_service(self, loop: asyncio.AbstractEventLoop,
                   *args: Any, **kwargs: Any) -> ServiceT:
        """Return the service this command should execute.

        For the worker we simply start the application itself.

        Note:
            The application will be started using a :class:`faust.Worker`.
        """
        self._init_worker_options(*args, **kwargs)
        return self.app

    def _init_worker_options(self,
                             *args: Any,
                             with_web: bool,
                             web_port: Optional[int],
                             web_bind: Optional[str],
                             web_host: str,
                             web_transport: URL,
                             **kwargs: Any) -> None:
        self.app.conf.web_enabled = with_web
        if web_port is not None:
            self.app.conf.web_port = web_port
        if web_bind:
            self.app.conf.web_bind = web_bind
        if web_host is not None:
            self.app.conf.web_host = web_host
        if web_transport is not None:
            self.app.conf.web_transport = web_transport

    @property
    def _Worker(self) -> Type[Worker]:
        # using Faust worker to start the app, not command code.
        return cast(Type[Worker], self.app.conf.Worker)

    def banner(self, worker: Worker) -> str:
        """Generate the text banner emitted before the worker starts."""
        return self._format_banner_table(
            self._banner_data(worker))

    def _format_banner_table(self, data: TableDataT) -> str:
        table = self.table(
            [(self.bold(x), str(y)) for x, y in data],
            title=self._banner_title(),
        )
        table.inner_heading_row_border = False
        table.inner_row_border = False
        return table.table

    def _banner_title(self) -> str:
        return self.faust_ident()

    def _banner_data(self, worker: Worker) -> TableDataT:
        app = cast(FaustWorker, worker).app
        logfile = worker.logfile if worker.logfile else '-stderr-'
        loglevel = level_name(worker.loglevel or 'WARN').lower()
        transport_extra = self._human_transport_info(worker.loop)
        return list(filter(None, [
            ('id', app.conf.id),
            ('transport', f'{app.conf.broker} {transport_extra}'),
            ('store', f'{app.conf.store}'),
            ('web', f'{app.web.url}') if app.conf.web_enabled else None,
            ('log', f'{logfile} ({loglevel})'),
            ('pid', f'{os.getpid()}'),
            ('hostname', f'{socket.gethostname()}'),
            ('platform', self.platform()),
            self._human_cython_info(),
            ('drivers', ''),
            ('  transport', app.transport.driver_version),
            ('  web', app.web.driver_version),
            ('datadir', f'{str(app.conf.datadir.absolute()):<40}'),
            ('appdir', f'{str(app.conf.appdir.absolute()):<40}'),
        ]))

    def _human_cython_info(self) -> Optional[Tuple[str, str]]:
        try:
            import faust._cython.windows  # noqa: F401
        except ImportError:
            return None
        else:
            compiler = platform.python_compiler()
            return ('       +', f'Cython ({compiler})')

    def _human_transport_info(self, loop: Any) -> str:
        # uvloop didn't leave us with any way to identify itself,
        # and also there's no uvloop.__version__ attribute.
        if loop.__class__.__module__ == 'uvloop':
            return '+uvloop'
        return ''

    def _driver_versions(self, app: AppT) -> List[str]:
        return [
            app.transport.driver_version,
            app.web.driver_version,
        ]

    def faust_ident(self) -> str:
        """Return Faust version information as ANSI string."""
        return self.color('hiblue', f'{FAUST} v{faust_version}')

    def platform(self) -> str:
        """Return platform identifier as ANSI string."""
        return '{py_imp} {py_version} ({system} {machine})'.format(
            py_imp=platform.python_implementation(),
            py_version=platform.python_version(),
            system=platform.system(),
            machine=platform.machine(),
        )
