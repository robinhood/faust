import click
import os
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Mapping, Sequence
from ..types.app import AppT
from ..utils.imports import import_from_cwd, symbol_by_name
from ..worker import DEBUG, DEFAULT_BLOCKING_TIMEOUT


common_options = [
    click.option('--app', '-A',
                 help='Path to Faust application to use.'),
    click.option('--quiet/--no-quiet', '-q', default=False,
                 help='Do not output warnings to stdout/stderr.'),
    click.option('--debug/--no-debug', default=DEBUG,
                 help='Enable debugging output'),
    click.option('--workdir',
                 help='Change working directory'),
]


worker_options = [
    click.option('--logfile', '-f', default=None,
                 help='Path to logfile (stderr is used by default)'),
    click.option('--loglevel', '-l', default='WARN',
                 help='Logging level to use: CRIT|ERROR|WARN|INFO|DEBUG'),
    click.option('--blocking-timeout',
                 default=DEFAULT_BLOCKING_TIMEOUT, type=float,
                 help='Blocking detector timeout (requires --debug)'),
    click.option('--web-port',
                 default=8080, type=int,
                 help='Port to run webserver on'),
]


def _apply_options(options: Sequence[Callable]) -> Callable:
    def _inner(fun: Callable) -> Callable:
        for opt in options:
            fun = opt(fun)
        return fun
    return _inner


@click.group()
@_apply_options(common_options)
@click.pass_context
def cli(ctx: click.Context,
        app: str,
        quiet: bool,
        debug: bool,
        workdir: str) -> None:
    ctx.obj = {
        'app': app,
        'quiet': quiet,
        'debug': debug,
        'workdir': workdir,
    }
    if workdir:
        os.chdir(Path(workdir).absolute())


@cli.command(help='Start worker')
@_apply_options(worker_options)
@click.pass_context
def worker(ctx: click.Context, logfile: str, loglevel: str) -> None:
    app = ctx.obj['app']
    debug = ctx.obj['debug']
    quiet = ctx.obj['quiet']
    if not app:
        raise click.UsageError('Need to specify app using -A parameter')
    app = find_app(app)
    from ..worker import Worker
    Worker(app,
           debug=debug,
           quiet=quiet,
           logfile=logfile,
           loglevel=loglevel).execute_from_commandline()


@click.command()
@_apply_options(common_options)
@_apply_options(worker_options)
def parse_worker_args(app: str, **kwargs: Any) -> Mapping:
    return kwargs


def find_app(app: str,
             *,
             symbol_by_name: Callable = symbol_by_name,
             imp: Callable = import_from_cwd) -> AppT:
    """Find app by name."""
    try:
        sym = symbol_by_name(app, imp=imp)
    except AttributeError:
        # last part was not an attribute, but a module
        sym = imp(app)
    if isinstance(sym, ModuleType) and ':' not in app:
        found = sym.app  # type: ignore
        if isinstance(found, ModuleType):
            raise AttributeError()
        return found
    return sym
