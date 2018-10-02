"""Program ``faust worker`` used to start application from console."""
import os
import platform
import socket
import typing
from typing import Any, Iterable, Optional

import click
from mode.utils.imports import symbol_by_name
from mode.utils.logging import level_name

from faust.types._env import BLOCKING_TIMEOUT, WEB_BIND, WEB_PORT

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
    """Start worker instance for given app."""

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
        option('--with-web/--without-web',
               default=True,
               help='Enable/disable web server and related components.'),
        option('--web-port', '-p',
               default=None, type=TCPPort(),
               help=f'Port to run web server on (default: {WEB_PORT})'),
        option('--web-bind', '-b', type=str),
        option('--web-host', '-h',
               default=socket.gethostname(), type=str,
               help=f'Canonical host name for the web server '
                    f'(default: {WEB_BIND})'),
        option('--console-port',
               default=50101, type=TCPPort(),
               help='(when --debug:) Port to run debugger console on.'),
    ]

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.start_worker(
            *self.args + args,
            **{**self.kwargs, **kwargs})

    def start_worker(self,
                     logfile: str,
                     loglevel: str,
                     blocking_timeout: float,
                     with_web: bool,
                     web_port: Optional[int],
                     web_bind: Optional[str],
                     web_host: str,
                     console_port: int) -> Any:
        self.app.conf.web_enabled = with_web
        if web_port is not None:
            self.app.conf.web_port = web_port
        if web_bind:
            self.app.conf.web_bind = web_bind
        if web_host is not None:
            self.app.conf.web_host = web_host
        worker = self.app.Worker(
            debug=self.debug,
            quiet=self.quiet,
            logfile=logfile,
            loglevel=loglevel,
            console_port=console_port,
        )
        self.say(self.banner(worker))
        return worker.execute_from_commandline()

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
        data = list(filter(None, [
            ('id', app.conf.id),
            ('transport', f'{app.conf.broker} {transport_extra}'),
            ('store', app.conf.store),
            ('web', app.web.url) if app.conf.web_enabled else None,
            ('log', f'{logfile} ({loglevel})'),
            ('pid', f'{os.getpid()}'),
            ('hostname', f'{socket.gethostname()}'),
            ('platform', self.platform()),
            ('drivers', '{transport_v} {http_v}'.format(
                transport_v=app.transport.driver_version,
                http_v=app.web.driver_version)),
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

    def faust_ident(self) -> str:
        return self.color('hiblue', f'{FAUST} v{faust_version}')

    def platform(self) -> str:
        return '{py_imp} {py_version} ({system} {machine})'.format(
            py_imp=platform.python_implementation(),
            py_version=platform.python_version(),
            system=platform.system(),
            machine=platform.machine(),
        )
