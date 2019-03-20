"""Program ``faust worker`` used to start application from console."""
import asyncio
import os
import platform
import socket
from typing import Any, List, Optional, cast

from mode import ServiceT, Worker
from mode.utils.imports import symbol_by_name
from mode.utils.logging import level_name
from yarl import URL

from faust.types import AppT
from faust.types._env import WEB_BIND, WEB_PORT, WEB_TRANSPORT

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
        self.say(self.banner(worker))

    def as_service(self, loop: asyncio.AbstractEventLoop,
                   *args: Any, **kwargs: Any) -> ServiceT:
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
    def _Worker(self) -> Worker:
        # using Faust worker to start the app, not command code.
        return self.app.conf.Worker

    def banner(self, worker: Worker) -> str:
        """Generate the text banner emitted before the worker starts."""
        app = worker.app
        loop = worker.loop
        transport_extra = ''
        # uvloop didn't leave us with any way to identify itself,
        # and also there's no uvloop.__version__ attribute.
        if loop.__class__.__module__ == 'uvloop':
            transport_extra = '+uvloop'
        if 'gevent' in loop.__class__.__module__:
            transport_extra = '+gevent'
        logfile = worker.logfile if worker.logfile else '-stderr-'
        loglevel = level_name(worker.loglevel or 'WARN').lower()
        compiler = platform.python_compiler()
        cython_info = None
        try:
            import faust._cython.windows  # noqa: F401
        except ImportError:
            pass
        else:
            cython_info = ('       +', f'Cython ({compiler})')
        data = list(filter(None, [
            ('id', app.conf.id),
            ('transport', f'{app.conf.broker} {transport_extra}'),
            ('store', app.conf.store),
            ('web', app.web.url) if app.conf.web_enabled else None,
            ('log', f'{logfile} ({loglevel})'),
            ('pid', f'{os.getpid()}'),
            ('hostname', f'{socket.gethostname()}'),
            ('platform', self.platform()),
            cython_info if cython_info else None,
            ('drivers', ''),
            ('  transport', app.transport.driver_version),
            ('  web', app.web.driver_version),
            ('datadir', f'{str(app.conf.datadir.absolute()):<40}'),
            ('appdir', f'{str(app.conf.appdir.absolute()):<40}'),
        ]))
        table = self.table(
            [(self.bold(x), str(y)) for x, y in data],
            title=self.faust_ident(),
        )
        table.inner_heading_row_border = False
        table.inner_row_border = False
        return table.table

    def _driver_versions(self, app: AppT) -> List[str]:
        return [
            app.transport.driver_version,
            app.web.driver_version,
        ]

    def faust_ident(self) -> str:
        return self.color('hiblue', f'{FAUST} v{faust_version}')

    def platform(self) -> str:
        return '{py_imp} {py_version} ({system} {machine})'.format(
            py_imp=platform.python_implementation(),
            py_version=platform.python_version(),
            system=platform.system(),
            machine=platform.machine(),
        )
