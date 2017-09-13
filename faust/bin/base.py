import abc
import asyncio
import os
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Sequence, Type
import click
from ._env import DEBUG
from ..types import AppT, CodecArg, ModelT
from ..utils import json
from ..utils.compat import want_bytes
from ..utils.imports import import_from_cwd, symbol_by_name

__all__ = [
    'AppCommand',
    'Command',
    'cli',
    'common_options',
    'apply_options',
    'find_app',
]


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


def apply_options(options: Sequence[Callable]) -> Callable:
    def _inner(fun: Callable) -> Callable:
        for opt in options:
            fun = opt(fun)
        return fun
    return _inner


@click.group()
@apply_options(common_options)
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


class Command(abc.ABC):
    UsageError: Type[Exception] = click.UsageError

    debug: bool
    quiet: bool
    workdir: str

    def __init__(self, ctx: click.Context) -> None:
        self.ctx = ctx
        self.debug = self.ctx.obj['debug']
        self.quiet = self.ctx.obj['quiet']
        self.workdir = self.ctx.obj['workdir']

    @abc.abstractmethod
    async def run(self) -> Any:
        ...

    def __call__(self) -> Any:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.run())

    def say(self, *args: Any, **kwargs: Any) -> None:
        if not self.quiet:
            print(*args, **kwargs)

    def carp(self, s: Any, **kwargs: Any) -> None:
        if self.debug:
            print(f'#-- {s}', **kwargs)

    def dumps(self, obj: Any) -> str:
        return json.dumps(obj)


class AppCommand(Command):
    app: AppT
    key_serializer: CodecArg
    value_serialier: CodecArg

    def __init__(self, ctx: click.Context,
                 *,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = None) -> None:
        super().__init__(ctx)
        appstr = self.ctx.obj['app']
        if not appstr:
            raise self.UsageError('Need to specify app using -A parameter')
        app = find_app(appstr)
        app.origin = appstr
        self.debug = self.ctx.obj['debug']
        self.quiet = self.ctx.obj['quiet']
        self.key_serializer = key_serializer or self.app.key_serializer
        self.value_serializer = value_serializer or self.app.value_serializer

    def to_key(self, typ: str, key: str) -> Any:
        return self.to_model(typ, key, self.key_serializer)

    def to_value(self, typ: str, value: str) -> Any:
        return self.to_model(typ, value, self.value_serializer)

    def to_model(self, typ: str, value: str, serializer: CodecArg) -> Any:
        if typ:
            model: ModelT = self.import_relative_to_app(typ)
            return model.loads(
                want_bytes(value), default_serializer=serializer)
        return want_bytes(value)

    def import_relative_to_app(self, attr: str) -> Any:
        try:
            return symbol_by_name(attr)
        except ImportError:
            root, _, _ = self.app.origin.partition(':')
            return symbol_by_name(f'{root}.{attr}')

    def to_topic(self, entity: str) -> Any:
        if not entity:
            raise self.UsageError('Missing topic/@actor name')
        if entity.startswith('@'):
            return self.import_relative_to_app(entity[1:])
        return self.app.topic(entity)


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


__flake8_ModelT_is_used: ModelT
