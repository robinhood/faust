import asyncio
import json
import os
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Mapping, Sequence, Tuple
import click
from ..types import AppT, CodecArg, FutureMessage, K, ModelT, RecordMetadata, V
from ..utils.compat import want_bytes
from ..utils.imports import import_from_cwd, symbol_by_name
from ..web.base import DEFAULT_BIND, DEFAULT_PORT
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
    click.option('--advertised-host', '-h',
                 default=DEFAULT_BIND, type=str,
                 help='Advertised host for the webserver'),
    click.option('--web-port', '-p',
                 default=DEFAULT_PORT, type=int,
                 help='Port to run webserver on'),
    click.option('--with-uvloop/--without-uvloop',
                 help='Use fast uvloop event loop'),
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
def worker(ctx: click.Context,
           logfile: str,
           loglevel: str,
           blocking_timeout: float,
           advertised_host: str,
           web_port: int,
           with_uvloop: bool) -> None:
    if with_uvloop:
        from .. import use_uvloop
        use_uvloop()
    app, debug, quiet = _get_default_context(ctx)
    app.advertised_url = f'{advertised_host}:{web_port}'
    from ..worker import Worker
    Worker(
        app,
        debug=debug,
        quiet=quiet,
        logfile=logfile,
        loglevel=loglevel,
        web_port=web_port,
    ).execute_from_commandline()


def _get_default_context(ctx: click.Context) -> Tuple[AppT, bool, bool]:
    app = ctx.obj['app']
    if not app:
        raise click.UsageError('Need to specify app using -A parameter')
    app = find_app(app)
    return app, ctx.obj['debug'], ctx.obj['quiet']


def say(quiet: bool, *args: Any, **kwargs: Any) -> None:
    if not quiet:
        print(*args, **kwargs)


@cli.command(help='Delete local table state')
@click.pass_context
def reset(ctx: click.Context) -> None:
    app, _, quiet = _get_default_context(ctx)
    for table in app.tables.values():
        say(quiet,
            f'Removing database for table {table.name}...')
        table.reset_state()
    say(quiet, f'Removing file "{app.checkpoint_path}"...')
    app.checkpoints.reset_state()


@cli.command(help='Send message to actor/topic')
@click.pass_context
@click.option('--key-type', '-K',
              help='Name of model to serialize key into')
@click.option('--key-serializer',
              help='Override default serializer for key.')
@click.option('--value-type', '-V',
              help='Name of model to serialize value into')
@click.option('--value-serializer',
              help='Override default serializer for value.')
@click.option('--key', '-k',
              help='Key value')
@click.option('--partition', type=int, help='Specific partition to send to')
@click.argument('entity')
@click.argument('value', default=None, required=False)
def send(ctx: click.Context, entity: str,
         value: str = None,
         key: str = None,
         key_type: str = None,
         key_serializer: CodecArg = None,
         value_type: str = None,
         value_serializer: CodecArg = None,
         partition: int = None) -> None:
    appstr = ctx.obj['app']
    app, _, quiet = _get_default_context(ctx)
    key_serializer = key_serializer or app.key_serializer
    value_serializer = value_serializer or app.value_serializer

    k: K = _to_model(appstr, key_type, want_bytes(key), key_serializer)
    v: V = _to_model(appstr, value_type, want_bytes(value), value_serializer)

    topic = _topic_from_str(app, appstr, entity)

    loop = asyncio.get_event_loop()
    # start sending the message
    fut: FutureMessage = loop.run_until_complete(topic.send(
        key=k, value=v, partition=partition,
    ))
    # wait for Producer to flush buffer and return RecordMetadata
    res: RecordMetadata = loop.run_until_complete(fut)
    say(quiet, json.dumps(res._asdict()))
    # then gracefully stop the producer.
    loop.run_until_complete(app.producer.stop())


def _to_model(appstr: str, typ: str, value: bytes,
              serializer: CodecArg) -> Any:
    if typ:
        model: ModelT = _import_relative_to_app(appstr, typ)
        return model.loads(value, default_serializer=serializer)
    return value


def _import_relative_to_app(appstr: str, attr: str) -> Any:
    try:
        return symbol_by_name(attr)
    except ImportError:
        root, _, _ = appstr.partition(':')
        return symbol_by_name(f'{root}.{attr}')


def _topic_from_str(app: AppT, appstr: str, entity: str) -> Any:
    if not entity:
        raise click.UsageError('Missing topic/@actor name')
    if entity.startswith('@'):
        return _import_relative_to_app(appstr, entity[1:])
    return app.topic(entity)


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


__flake8_FutureMessage_is_used: FutureMessage    # XXX flake8 bug
__flake8_K_is_used: K
__flake8_ModelT_is_used: ModelT
__flake8_RecordMetadata_is_used: RecordMetadata
__flake8_V_is_used: V
