"""Common class for console commands."""
import abc
import asyncio
import os
from functools import wraps
from pathlib import Path
from types import ModuleType
from typing import (
    Any, Callable, ClassVar, List, Mapping, Sequence, Type, no_type_check,
)
import click
from tabulate import tabulate
from ._env import DEBUG
from ..types import AppT, CodecArg, ModelT
from ..utils import json
from ..utils.compat import want_bytes
from ..utils.imports import import_from_cwd, symbol_by_name

__all__ = [
    'AppCommand',
    'Command',
    'cli',
    'find_app',
]

# Extends the :pypi:`click` framework to use classes instead of
# function decorators. May regret, but the click API is so messy,
# with lots of function imports and dozens of decorators for each command,
# and these classes removes a ton of boilerplate.
# [ask]


# These are the options common to all commands in the :mod:`faust.bin.faust`
# umbrella command.
builtin_options: Sequence[Callable] = [
    click.option('--app', '-A',
                 help='Path to Faust application to use.'),
    click.option('--quiet/--no-quiet', '-q', default=False,
                 help='Do not output warnings to stdout/stderr.'),
    click.option('--debug/--no-debug', default=DEBUG,
                 help='Enable debugging output.'),
    click.option('--workdir',
                 help='Change working directory.'),
    click.option('--json/--no-json', default=False,
                 help='Output data in json format.'),
]


def find_app(app: str,
             *,
             symbol_by_name: Callable = symbol_by_name,
             imp: Callable = import_from_cwd) -> AppT:
    """Find app by string like ``examples.simple``.

    Notes:
        This function uses ``import_from_cwd`` to temporarily
        add the current working directory to :envvar:`PYTHONPATH`,
        such that when importing the app it will search the current
        working directory last, as if running with:

        .. sourcecode: console

            $ PYTHONPATH="${PYTHONPATH}:."

        You can control this with the ``imp`` keyword argument,
        for example passing ``imp=importlib.import_module``.

    Examples:

        >>> # Providing the name of a module will default to
        >>> # attribute named .app, and the below is the same as:
        >>> #    from examples.simple import app
        >>> find_app('examples.simple')

        >>> # If you want an attribute other than .app you can
        >>> # use colon to separate module and attribute, and the
        >>> # below is the same as:
        >>> #     from examples.simple import my_app
        >>> find_app('examples.simple:my_app')

        >>> # You can also use period for the module/attribute separator
        >>> find_app('examples.simple.my_app')

    """
    try:
        # Try to import name' as is.
        val = symbol_by_name(app, imp=imp)
    except AttributeError:
        # last part (of "pkg.x") was not an attribute,
        # but a module instead: use imp to import_module.
        val = imp(app)
    if isinstance(val, ModuleType) and ':' not in app:
        # if we found a module, try to get .app attribute
        found = val.app  # type: ignore
        if isinstance(found, ModuleType):
            # proj.app:x where x is a module
            raise AttributeError(f'Looks like module, not app: -A {app}')
        return found
    return val


# This is here for app.worker_start(argv) only, as it needs
# to parse the command-line arguments programmatically, something
# click doesn't seem to support easily [ask].
def _apply_options(options: Sequence[Callable]) -> Callable:
    """Add list of ``click.option`` values to click command function."""
    def _inner(fun: Callable) -> Callable:
        for opt in options:
            fun = opt(fun)
        return fun
    return _inner


class _Group(click.Group):

    # This is here for app.main(). It's used to disable
    # the (otherwise mandatory) '-A' command-line option.
    # `app.main() calls cli(app=self), which puts the app
    # on the click.Context, then AppCommand reads it from the context.

    @no_type_check  # mypy bugs out on this
    def make_context(self, info_name: str, args: str,
                     app: AppT = None,
                     parent: click.Context = None,
                     **extra: Any) -> click.Context:
        ctx = super().make_context(info_name, args, **extra)
        # hmm. this seems to be a stack
        ctx.find_root().app = app
        return ctx


# This is the thing that app.main(), ``python -m faust -A ...``,
# and ``faust -A ..`` calls (see also faust/__main__.py, and setup.py
# in the git repository (entrypoints).)

@click.group(cls=_Group)
@_apply_options(builtin_options)
@click.pass_context
def cli(ctx: click.Context,
        app: str,
        quiet: bool,
        debug: bool,
        workdir: str,
        json: bool) -> None:
    ctx.obj = {
        'app': app,
        'quiet': quiet,
        'debug': debug,
        'workdir': workdir,
        'json': json,
    }
    # XXX I'm not sure this is the best place to chdir [ask]
    if workdir:
        os.chdir(Path(workdir).absolute())


class Command(abc.ABC):
    UsageError: Type[Exception] = click.UsageError

    # To subclass this:
    #
    # You only need ``def run``, but see note in as_click_command.

    abstract: ClassVar[bool] = True
    _click: Any = None

    debug: bool
    quiet: bool
    workdir: str
    json: bool

    builtin_options: List = builtin_options
    options: List = None

    @classmethod
    def as_click_command(cls) -> Callable:
        # This is what actually registers the commands into the
        # :pypi:`click` command-line interface (the ``def cli`` main above).
        # __init_subclass__ automatically calls as_click_command
        # for the side effect of being registered in the list
        # of `faust` umbrella subcommands.
        @click.pass_context
        @wraps(cls)
        def _inner(*args: Any, **kwargs: Any) -> Callable:
            return cls(*args, **kwargs)()  # type: ignore
        return _apply_options(cls.options or [])(
            cli.command(help=cls.__doc__)(_inner))

    def __init_subclass__(self, *args: Any, **kwargs: Any) -> None:
        if self.abstract:
            # this sets the class attribute, so next time
            # a Command subclass is defined it won't be abstract
            # unless you set the attribute again in that subclass::
            #   class MyAbstractCommand(Command):
            #       abstract: ClassVar[bool] = True
            #
            #   class x(MyAbstractCommand):
            #       async def run(self) -> None:
            #           print('just here to experience this execution')
            self.abstract = False
        else:
            # This registers the command with the cli click.Group
            # as a side effect.
            self._click = self.as_click_command()

        # This hack creates the Command.parse method used by App.start_worker
        # to parse command-line arguments in sys.argv.
        # Unable to find a better way to do this in click. [ask]
        # Apparently the side effect of the @click.option decorator
        # is enough: you don't need reduction of the return value.
        _apply_options(self.builtin_options)(self._parse)
        _apply_options(self.options or [])(self._parse)

    @classmethod
    def parse(cls, argv: Sequence[str]) -> Mapping:
        """Parse command-line arguments in argv' and return mapping."""
        return cls._parse(argv, standalone_mode=False)

    @staticmethod
    @click.command()
    def _parse(**kwargs: Any) -> Mapping:
        return kwargs

    def __init__(self, ctx: click.Context) -> None:
        self.ctx = ctx
        # XXX should we also use ctx.find_root() here?
        self.debug = self.ctx.obj['debug']
        self.quiet = self.ctx.obj['quiet']
        self.workdir = self.ctx.obj['workdir']
        self.json = self.ctx.obj['json']

    async def run(self) -> Any:
        # NOTE: If you override __call__ below, you have a non-async command.
        # This is used by .worker to call the
        # Worker.execute_from_commandline() method.
        ...

    def __call__(self) -> Any:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.run())

    def tabulate(self, data: Sequence,
                 *,
                 headers: Any = 'firstrow',
                 **kwargs: Any) -> str:
        """Use the :pypi:`tabulate` library to create an ASCII table
        representation out of a sequence of tuples.

        Note:
            If the :option:`--json`` option is enabled this returns
            json instead.
        """
        if self.json:
            return self.dumps(data)
        return tabulate(data, headers=headers, **kwargs)

    def say(self, *args: Any, **kwargs: Any) -> None:
        """Print something to stdout (or use ``file=stderr`` kwarg).

        Note:
            Does not do anything if the :option:`--quiet` option is enabled.
        """
        if not self.quiet:
            print(*args, **kwargs)

    def carp(self, s: Any, **kwargs: Any) -> None:
        """Print something to stdout (or use ``file=stderr`` kwargs).

        Note:
            Does not do anything if the :option:`--debug` option is enabled.
        """
        if self.debug:
            print(f'#-- {s}', **kwargs)

    def dumps(self, obj: Any) -> str:
        # Shortcut! and can urm..,, override if json is not wanted output.
        return json.dumps(obj)


class AppCommand(Command):
    """Command that takes ``-A app`` as argument."""

    abstract: ClassVar[bool] = True

    app: AppT

    #: The :term:`codec` used to serialize keys.
    #: Taken from instance parameters or :attr:`@key_serializer`.
    key_serializer: CodecArg

    #: The :term:`codec` used to serialize values.
    #: Taken from instance parameters or :attr:`@value_serializer`.
    value_serialier: CodecArg

    def __init__(self, ctx: click.Context,
                 # we pass starargs to init_options [VVV]
                 *args: Any,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = None,
                 **kwargs: Any) -> None:
        # and also starkwargs [^^^]
        super().__init__(ctx)  # Command! Remember?

        # App is taken from context first (see _Group)
        # XXX apparently click.Context is a stack?,
        # not sure why I have to find the root here [ask]
        self.app = getattr(ctx.find_root(), 'app', None)
        if self.app is None:
            appstr = self.ctx.obj['app']
            if not appstr:
                raise self.UsageError('Need to specify app using -A parameter')
            self.app = find_app(appstr)
        else:
            appstr = '__main__'
        self.app.origin = appstr
        self.key_serializer = key_serializer or self.app.key_serializer
        self.value_serializer = value_serializer or self.app.value_serializer
        self.init_options(*args, **kwargs)

    def init_options(self, *args: Any, **kwargs: Any) -> None:
        """You can override this to add attributes from init starargs."""
        if args:
            raise TypeError(f'Unexpected positional arguments: {args!r}')
        if kwargs:
            raise TypeError(f'Unexpected keyword arguments: {kwargs!r}')

    def to_key(self, typ: str, key: str) -> Any:
        """Convert command-line argument string to model (key).

        Arguments:
            typ: The name of the model to create.
            key: The string json of the data to populate it with.

        Notes:
            Uses :attr:`key_serializer` to set the :term:`codec`
            for the key (e.g. ``"json"``), as set by the
            :option:`--key-serializer` option.
        """
        return self.to_model(typ, key, self.key_serializer)

    def to_value(self, typ: str, value: str) -> Any:
        """Convert command-line argument string to model (value).

        Arguments:
            typ: The name of the model to create.
            key: The string json of the data to populate it with.
        Notes:
            Uses :attr:`value_serializer` to set the :term:`codec`
            for the value (e.g. ``"json"``), as set by the
            :option:`--value-serializer` option.
        """
        return self.to_model(typ, value, self.value_serializer)

    def to_model(self, typ: str, value: str, serializer: CodecArg) -> Any:
        """Generic version of :meth:`to_key`/:meth:`to_value`.

        Arguments:
            typ: The name of the model to create.
            key: The string json of the data to populate it with.
            serializer: The argument setting it apart from to_key/to_value
                enables you to specify a custom serializer not mandated
                by :attr:`key_serializer`, and :attr:`value_serializer`.
        Notes:
            Uses :attr:`value_serializer` to set the :term:`codec`
            for the value (e.g. ``"json"``), as set by the
            :option:`--value-serializer` option.
        """
        if typ:
            model: ModelT = self.import_relative_to_app(typ)
            return model.loads(
                want_bytes(value), default_serializer=serializer)
        return want_bytes(value)

    def import_relative_to_app(self, attr: str) -> Any:
        """Import string like "module.Model", or "Model" to model class."""
        try:
            return symbol_by_name(attr)
        except ImportError:
            root, _, _ = self.app.origin.partition(':')
            return symbol_by_name(f'{root}.{attr}')

    def to_topic(self, entity: str) -> Any:
        """Convert topic name given on command-line to ``app.topic()``."""
        if not entity:
            raise self.UsageError('Missing topic/@actor name')
        if entity.startswith('@'):
            return self.import_relative_to_app(entity[1:])
        return self.app.topic(entity)


__flake8_ModelT_is_used: ModelT  # XXX: flake8 bug
