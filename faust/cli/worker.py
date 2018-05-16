"""Program ``faust worker`` used to start application from console."""
import os
import platform
import socket
import typing
from typing import Any, Iterable, Optional

import click
from mode.utils.imports import symbol_by_name
from mode.utils.logging import level_name
from yarl import URL

from ._env import BLOCKING_TIMEOUT, WEB_BIND, WEB_PORT
from .base import AppCommand, TCPPort, WritableFilePath, option

if typing.TYPE_CHECKING:
    from faust.worker import Worker
else:
    class Worker: ...   # noqa

__all__ = ['worker']

FAUST = 'ƒaµS†'

# XXX mypy borks if we do `from faust import __version`.
faust_version: str = symbol_by_name('faust:__version__')

LOGLEVELS = (
    'CRIT',
    'ERROR',
    'WARN',
    'INFO',
    'DEBUG',
)

DEFAULT_LOGLEVEL = 'WARN'


class CaseInsensitiveChoice(click.Choice):

    def __init__(self, choices: Iterable[Any]) -> None:
        self.choices = [str(val).lower() for val in choices]

    def convert(self,
                value: str,
                param: Optional[click.Parameter],
                ctx: Optional[click.Context]) -> Any:
        if value.lower() in self.choices:
            return value
        return super().convert(value, param, ctx)


class worker(AppCommand):
    """Start ƒaust worker instance."""

    options = [
        option('--logfile', '-f',
               default=None, type=WritableFilePath,
               help='Path to logfile (default is <stderr>).'),
        option('--loglevel', '-l',
               default=DEFAULT_LOGLEVEL, type=CaseInsensitiveChoice(LOGLEVELS),
               help='Logging level to use.'),
        option('--blocking-timeout',
               default=BLOCKING_TIMEOUT, type=float,
               help='Blocking detector timeout (requires --debug).'),
        option('--web-port', '-p',
               default=WEB_PORT, type=TCPPort(),
               help='Port to run web server on.'),
        option('--web-bind', '-b', default=WEB_BIND, type=str),
        option('--web-host', '-h',
               default=socket.gethostname(), type=str,
               help='Canonical host name for the web server.'),
        option('--console-port',
               default=50101, type=TCPPort(),
               help='(when --debug:) Port to run debugger console on.'),
    ]

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.start_worker(
            *self.args + args,
            **{**self.kwargs, **kwargs})

    def start_worker(self, logfile: str, loglevel: str,
                     blocking_timeout: float, web_port: int, web_bind: str,
                     web_host: str, console_port: int) -> Any:
        self.app.conf.canonical_url = URL(f'http://{web_host}:{web_port}')
        worker = self.app.Worker(
            debug=self.debug,
            quiet=self.quiet,
            logfile=logfile,
            loglevel=loglevel,
            web_port=web_port,
            web_bind=web_bind,
            web_host=web_host,
            console_port=console_port,
        )
        self.say(self.banner(worker))
        return worker.execute_from_commandline()

    def banner(self, worker: Worker) -> str:
        """Generate the text banner emitted before the worker starts."""
        app = worker.app
        loop = worker.loop
        website = worker.website
        transport_extra = ''
        # uvloop didn't leave us with any way to identify itself,
        # and also there's no uvloop.__version__ attribute.
        if loop.__class__.__module__ == 'uvloop':
            transport_extra = '+uvloop'
        if 'gevent' in loop.__class__.__module__:
            transport_extra = '+gevent'
        logfile = worker.logfile if worker.logfile else '-stderr-'
        loglevel = level_name(worker.loglevel or 'WARN').lower()
        data = [
            ('id', app.conf.id),
            ('transport', f'{app.conf.broker} {transport_extra}'),
            ('store', app.conf.store),
            ('web', website.web.url),
            ('log', f'{logfile} ({loglevel})'),
            ('pid', f'{os.getpid()}'),
            ('hostname', f'{socket.gethostname()}'),
            ('platform', self.platform()),
            ('drivers', '{transport_v} {http_v}'.format(
                transport_v=app.transport.driver_version,
                http_v=website.web.driver_version)),
            ('datadir', f'{str(app.conf.datadir.absolute()):<40}'),
            ('appdir', f'{str(app.conf.appdir.absolute()):<40}'),
        ]
        table = self.table(
            [(self.bold(x), y) for x, y in data],
            title=self.faust_ident(),
        )
        table.inner_heading_row_border = False
        table.inner_row_border = False
        return table.table

    def faust_ident(self) -> str:
        return self.color('hiblue', f'{FAUST} v{faust_version}')

    def platform(self) -> str:
        return '{py_imp} {py_version} ({system} {machine})'.format(
            py_imp=platform.python_implementation(),
            py_version=platform.python_version(),
            system=platform.system(),
            machine=platform.machine(),
        )
