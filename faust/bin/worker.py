"""Program ``faust worker`` used to start application from console.

.. program:: faust worker

.. cmdoption:: --logfile, -f

    Path to logfile (default is <stderr>).

.. cmdoption:: --loglevel, -l

    Logging level to use: CRIT|ERROR|WARN|INFO|DEBUG.

.. cmdoption:: --blocking-timeout

    Blocking detector timeout (requires --debug).

.. cmdoption:: --advertised-host, -h

    Advertised host for the web server.

.. cmdoption:: --web-port, -p

    Port to run web server on.

.. cmdoption:: --with-uvloop, --without-uvloop

    Use uvloop event loop.
"""
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
        option('--advertised-host', '-h',
               default=WEB_BIND, type=str,
               help='Advertised host for the web server.'),
        option('--web-port', '-p', default=WEB_PORT, type=int,
               help='Port to run web server on.'),
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
                     advertised_host: str,
                     web_port: int) -> None:
        self.logfile = logfile
        self.loglevel = loglevel
        self.blocking_timeout = blocking_timeout
        self.advertised_host = advertised_host
        self.web_port = web_port

    def __call__(self) -> Any:
        from ..worker import Worker
        self.app.advertised_url = f'{self.advertised_host}:{self.web_port}'
        return Worker(
            self.app,
            debug=self.debug,
            quiet=self.quiet,
            logfile=self.logfile,
            loglevel=self.loglevel,
            web_port=self.web_port,
        ).execute_from_commandline()
