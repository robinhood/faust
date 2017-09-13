from typing import Any, Mapping
import click
from ._env import DEFAULT_BLOCKING_TIMEOUT, WEB_BIND, WEB_PORT
from .base import AppCommand, apply_options, cli, common_options

__all__ = [
    'worker',
    'worker_options',
    'parse_worker_args',
]

worker_options = [
    click.option('--logfile', '-f', default=None,
                 help='Path to logfile (stderr is used by default)'),
    click.option('--loglevel', '-l', default='WARN',
                 help='Logging level to use: CRIT|ERROR|WARN|INFO|DEBUG'),
    click.option('--blocking-timeout',
                 default=DEFAULT_BLOCKING_TIMEOUT, type=float,
                 help='Blocking detector timeout (requires --debug)'),
    click.option('--advertised-host', '-h',
                 default=WEB_BIND, type=str,
                 help='Advertised host for the webserver'),
    click.option('--web-port', '-p',
                 default=WEB_PORT, type=int,
                 help='Port to run webserver on'),
    click.option('--with-uvloop/--without-uvloop',
                 help='Use fast uvloop event loop'),
]


@cli.command(help='Start worker')
@apply_options(worker_options)
@click.pass_context
class worker(AppCommand):

    def __init__(self,
                 ctx: click.Context,
                 logfile: str,
                 loglevel: str,
                 blocking_timeout: float,
                 advertised_host: str,
                 web_port: int,
                 with_uvloop: bool) -> None:
        if with_uvloop:
            from .. import use_uvloop
            use_uvloop()
        self.logfile = logfile
        self.loglevel = loglevel
        self.blocking_timeout = blocking_timeout
        self.web_port = web_port

        super().__init__(ctx)
        self.app.advertised_url = f'{advertised_host}:{web_port}'
        self()

    def __call__(self) -> Any:
        from ..worker import Worker
        return Worker(
            self.app,
            debug=self.debug,
            quiet=self.quiet,
            logfile=self.logfile,
            loglevel=self.loglevel,
            web_port=self.web_port,
        ).execute_from_commandline()


@click.command()
@apply_options(common_options)
@apply_options(worker_options)
def parse_worker_args(app: str, **kwargs: Any) -> Mapping:
    return kwargs
