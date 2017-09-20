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
import socket
from typing import Any
from ._env import BLOCKING_TIMEOUT, WEB_BIND, WEB_PORT
from .base import AppCommand, option

__all__ = ['worker']


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

    def init_options(self,
                     logfile: str,
                     loglevel: str,
                     blocking_timeout: float,
                     web_port: int,
                     web_bind: str,
                     web_host: str) -> None:
        self.logfile = logfile
        self.loglevel = loglevel
        self.blocking_timeout = blocking_timeout
        self.web_port = web_port
        self.web_bind = web_bind
        self.web_host = web_host

    def __call__(self) -> Any:
        from ..worker import Worker
        self.app.canonical_url = f'{self.web_host}:{self.web_port}'
        return Worker(
            self.app,
            debug=self.debug,
            quiet=self.quiet,
            logfile=self.logfile,
            loglevel=self.loglevel,
            web_port=self.web_port,
            web_bind=self.web_bind,
            web_host=self.web_host,
        ).execute_from_commandline()
