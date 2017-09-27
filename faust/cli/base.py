"""Common class for console commands."""
import abc
import asyncio
import os
import sys
from functools import wraps
from pathlib import Path
from textwrap import wrap
from types import ModuleType
from typing import (
    Any, Callable, ClassVar, Dict, List,
    Mapping, Sequence, Tuple, Type, no_type_check,
)
import click
from colorclass import Color, disable_all_colors, enable_all_colors
from terminaltables import SingleTable
from terminaltables.base_table import BaseTable
from ._env import DATADIR, DEBUG, WORKDIR
from ..types import AppT, CodecArg, ModelT
from ..utils import json
from ..utils.compat import want_bytes
from ..utils.imports import import_from_cwd, symbol_by_name

__all__ = [
    'AppCommand',
    'Command',
    'argument',
    'cli',
    'find_app',
    'option',
]

# This module extends the :pypi:`click` framework to use classes instead of
# function decorators. May regret, but there was lots of repetition
# with lots of function imports and dozens of decorators for each command.
# The Command and AppCommand classes makes it a lot easier to write
# commands and reuse functionality.  [ask]


# here so we can import option/argument from this module.
argument = click.argument
option = click.option


# Options common to all commands
builtin_options: Sequence[Callable] = [
    option('--app', '-A',
           help='Path of Faust application to use, or the name of a module.'),
    option('--quiet/--no-quiet', '-q', default=False,
           help='Silence output to <stdout>/<stderr>.'),
    option('--debug/--no-debug', default=DEBUG,
           help='Enable debugging output, and the blocking detector.'),
    option('--color/--no-color', default=True,
           help='Enable colors in output.'),
    option('--workdir', '-W', default=WORKDIR,
           help='Working directory to change to after start.'),
    option('--datadir', default=DATADIR,
           help='Directory to keep application state.'),
    option('--json/--no-json', default=False,
           help='Prefer data to be emitted in json format.'),
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
        working directory last.

        You can think of it as temporarily
        running with the :envvar:`PYTHONPATH` set like this:

        .. sourcecode: console

            $ PYTHONPATH="${PYTHONPATH}:."

        You can disable this with the ``imp`` keyword argument,
        for example passing ``imp=importlib.import_module``.

    Examples:

        >>> # If providing the name of a module, it will attempt
        >>> # to find an attribute name .app in that module, so
        >>> # the below is the same as importing::
        >>> #    from examples.simple import app
        >>> find_app('examples.simple')

        >>> # If you want an attribute other than .app you can
        >>> # use colon to separate module and attribute.
        >>> # The below is the same as importing::
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


# We just use this to apply many @click.option/@click.argument
# decorators in the same decorator.
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

    def get_help(self, ctx: click.Context) -> str:
        self._maybe_import_app()
        return super().get_help(ctx)

    def get_usage(self, ctx: click.Context) -> str:
        self._maybe_import_app()
        return super().get_usage(ctx)

    def _maybe_import_app(self, argv: Sequence[str] = sys.argv) -> None:
        # This is here so that custom AppCommand defined in example/myapp.py
        # works and is included in --help/usage, etc. when using the faust
        # command like:
        #   $ faust -A example.myapp --help
        #
        # This is not necessary when using app.main(), since that always
        # imports the app module before creating the cli() object:
        #   $ python example/myapp.py --help
        for i, arg in enumerate(argv):
            if arg == '-A':
                try:
                    find_app(argv[i + 1])
                except IndexError:
                    raise click.UsageError('Missing argument for -A')
            elif arg.startswith('--app'):
                if '=' in arg:
                    _, _, value = arg.partition('=')
                    find_app(value)
                else:
                    try:
                        find_app(argv[i + 1])
                    except IndexError:
                        raise click.UsageError('Missing argument for --app')

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
        datadir: str,
        json: bool,
        color: bool) -> None:
    ctx.obj = {
        'app': app,
        'quiet': quiet,
        'debug': debug,
        'workdir': workdir,
        'datadir': datadir,
        'json': json,
        'color': color,
    }
    if workdir:
        os.environ['F_WORKDIR'] = workdir
        # XXX I'm not sure this is the best place to chdir [ask]
        os.chdir(Path(workdir).absolute())
    if datadir:
        # This is the only way we can set the datadir for App.__init__,
        # so that default values will have the right path prefix.
        # WARNING: It's crucial the app module is imported later than this.
        #          If the app is imported first some paths may have the
        #          default prefix, while others have the wanted prefix.
        os.environ['F_DATADIR'] = datadir
    if color:
        enable_all_colors()
    else:
        disable_all_colors()


class Command(abc.ABC):
    UsageError: Type[Exception] = click.UsageError

    # To subclass this you only need to define:
    #
    # run for an async command:
    #
    #     async def run(self, *args, **kwargs) -> None:
    #         ...
    # or for a non-async command you override __call__:
    #
    #     def __call__(self, *args, **kwargs) -> Any:
    #         ...

    abstract: ClassVar[bool] = True
    _click: Any = None

    debug: bool
    quiet: bool
    workdir: str
    datadir: str
    json: bool
    color: bool

    builtin_options: List = builtin_options
    options: List = None

    args: Tuple = None
    kwargs: Dict = None

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
            cmd = cls(*args, **kwargs)  # type: ignore
            return cmd()
        return _apply_options(cls.options or [])(
            cli.command(help=cls.__doc__)(_inner))

    def __init_subclass__(self, *args: Any, **kwargs: Any) -> None:
        if self.abstract:
            # this sets the class attribute, so next time
            # a Command subclass is defined it will have abstract=False,
            # That is unless you set the attribute again in that subclass::
            #   class MyAbstractCommand(Command):
            #       abstract: ClassVar[bool] = True
            #
            #   class x(MyAbstractCommand):
            #       async def run(self) -> None:
            #           print('just here to experience this execution')
            self.abstract = False
        else:
            # we use this for the side effect: as_click_command registers the
            # command with the ``cli`` click.Group.
            self._click = self.as_click_command()

        # This hack creates the Command.parse method,
        # that was used by App.start_worker (now removed)
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

    def __init__(self, ctx: click.Context, *args: Any, **kwargs: Any) -> None:
        self.ctx = ctx
        # XXX should we also use ctx.find_root() here?
        self.debug = self.ctx.obj['debug']
        self.quiet = self.ctx.obj['quiet']
        self.workdir = self.ctx.obj['workdir']
        self.datadir = self.ctx.obj['datadir']
        self.json = self.ctx.obj['json']
        self.color = self.ctx.obj['color']
        self.args = args
        self.kwargs = kwargs

    @no_type_check   # Subclasses can omit *args, **kwargs in signature.
    async def run(self, *args: Any, **kwargs: Any) -> Any:
        # NOTE: If you override __call__ below, you have a non-async command.
        # This is used by .worker to call the
        # Worker.execute_from_commandline() method.
        ...

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_event_loop()
        args = self.args + args
        kwargs = {**self.kwargs, **kwargs}
        return loop.run_until_complete(self.run(*args, **kwargs))

    def tabulate(self, data: Sequence[Sequence[str]],
                 headers: Sequence[str] = None,
                 wrap_last_row: bool = True,
                 title: str = None,
                 title_color: str = 'blue',
                 **kwargs: Any) -> str:
        """Creates an ANSI representation of a table of two-row tuples.

        See Also:
            Keyword arguments are forwarded to
            :class:`terminaltables.SingleTable`

        Note:
            If the :option:`--json` option is enabled this returns
            json instead.
        """
        if self.json:
            return self.dumps(data)
        if headers:
            data = [headers] + list(data)
        title = self.bold(self.colored(title_color, title))
        table = SingleTable(data, title=title, **kwargs)
        if wrap_last_row:
            # slow, but not big data
            data = [
                list(l[:-1]) + [self._table_wrap(table, l[-1])]
                for l in data
            ]
        return table.table

    def colored(self, color: str, text: str) -> str:
        return Color(f'{{{color}}}{text}{{/{color}}}')

    def bold(self, text: str) -> str:
        return self.colored('b', text)

    def _table_wrap(self, table: BaseTable, text: str) -> str:
        max_width = table.column_max_width(1)
        return '\n'.join(wrap(text, max_width))

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
                 # we keep starargs in self.args attribute [VVV]
                 *args: Any,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = None,
                 **kwargs: Any) -> None:
        # and also starkwargs in self.kwargs [^^^]
        super().__init__(ctx)

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
        self.args = args
        self.kwargs = kwargs

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
