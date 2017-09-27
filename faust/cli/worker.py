"""Program ``faust worker`` used to start application from console.

.. program:: faust worker

.. cmdoption:: --logfile, -f

    Path to logfile (default is <stderr>).

.. cmdoption:: --loglevel, -l

    Logging level to use: CRIT|ERROR|WARN|INFO|DEBUG.

.. cmdoption:: --blocking-timeout

    Blocking detector timeout (requires --debug).

.. cmdoption:: --web-host, -h

    Canonical host name for the web server.

.. cmdoption:: --web-port, -p

    Port to run web server on.

.. cmdoption:: --with-uvloop, --without-uvloop

    Use uvloop event loop.
"""
import os
import platform
import socket
import typing
from typing import Any
from mode.utils.logging import level_name
from terminaltables import SingleTable
from yarl import URL
from ._env import BLOCKING_TIMEOUT, WEB_BIND, WEB_PORT
from .base import AppCommand, option
from .. import __version__ as faust_version

if typing.TYPE_CHECKING:
    from ..worker import Worker
else:
    class Worker: ...   # noqa

__all__ = ['worker']

FAUST = 'ƒaµS†'


class worker(AppCommand):
    """Start worker instance."""

    options = [
        option('--logfile', '-f', default=None,
               help='Path to logfile (default is <stderr>).'),
        option('--loglevel', '-l', default='WARN',
               help='Logging level to use: CRIT|ERROR|WARN|INFO|DEBUG.'),
        option('--blocking-timeout',
               default=BLOCKING_TIMEOUT, type=float,
               help='Blocking detector timeout (requires --debug).'),
        option('--web-port', '-p', default=WEB_PORT, type=int,
               help='Port to run web server on.'),
        option('--web-bind', '-b', default=WEB_BIND, type=str),
        option('--web-host', '-h',
               default=socket.gethostname(), type=str,
               help='Canonical host name for the web server.'),
        option('--with-uvloop/--without-uvloop',
               help='Use fast uvloop event loop.'),
    ]

    def __init__(self, *args: Any,
                 with_uvloop: bool = None,
                 **kwargs: Any) -> None:
        if with_uvloop:
            from .. import use_uvloop
            use_uvloop()
        super().__init__(*args, **kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.start_worker(
            *self.args + args,
            **{**self.kwargs, **kwargs})

    def start_worker(self,
                     logfile: str,
                     loglevel: str,
                     blocking_timeout: float,
                     web_port: int,
                     web_bind: str,
                     web_host: str) -> Any:
        self.app.canonical_url = URL(f'http://{web_host}:{web_port}')
        worker = self.app.Worker(
            debug=self.debug,
            quiet=self.quiet,
            logfile=logfile,
            loglevel=loglevel,
            web_port=web_port,
            web_bind=web_bind,
            web_host=web_host,
        )
        self.say(self.banner(worker))
        return worker.execute_from_commandline()

    def banner(self, worker: Worker) -> str:
        'Generate the text banner emitted before the worker starts.'
        app = worker.app
        loop = worker.loop
        website = worker.website
        transport_extra = ''
        # uvloop didn't leave us with any way to identify itself,
        # and also there's no uvloop.__version__ attribute.
        if loop.__class__.__module__ == 'uvloop':
            transport_extra = '+uvloop'
        logfile = worker.logfile if worker.logfile else '-stderr-'
        loglevel = level_name(worker.loglevel or 'WARN').lower()
        data = [
            ('id', app.id),
            ('transport', f'{app.url} {transport_extra}'),
            ('store', app.store),
            ('web', website.web.url),
            ('log', f'{logfile} ({loglevel})'),
            ('pid', f'{os.getpid()}'),
            ('hostname', f'{socket.gethostname()}'),
            ('platform', self.platform()),
            ('drivers', '{transport_v} {http_v}'.format(
                transport_v=app.transport.driver_version,
                http_v=website.web.driver_version)),
            ('datadir', f'{str(app.datadir.absolute()):<40}'),
        ]
        table = SingleTable(
            [(self.bold(x), y) for x, y in data],
            title=self.faust_ident(),
        )
        table.inner_heading_row_border = False
        table.inner_row_border = False
        return table.table

    def faust_ident(self) -> str:
        return self.colored('hiblue', f'{FAUST} v{faust_version}')

    def platform(self) -> str:
        return '{py_imp} {py_v} ({system} {machine})'.format(
            py=platform.python_implementation(),
            system=platform.system(),
            py_v=platform.python_version(),
            py_imp=platform.python_implementation(),
            machine=platform.machine(),
        )
