"""Command-line programs using :pypi:`click`."""
import abc
import asyncio
import inspect
import os
import signal
import sys
from functools import wraps
from pathlib import Path
from textwrap import wrap
from types import ModuleType
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    no_type_check,
)

import click
from click import echo
from colorclass import Color, disable_all_colors, enable_all_colors
from mode.utils import text
from mode.utils.compat import want_bytes
from mode.utils.imports import import_from_cwd, symbol_by_name

from faust.types import AppT, CodecArg, ModelT
from faust.utils import json
from faust.utils import terminal

from ._env import DATADIR, DEBUG, WORKDIR

try:
    import click_completion
except ImportError:
    click_completion = None
else:
    click_completion.init()

__all__ = [
    'AppCommand',
    'Command',
    'TCPPort',
    'WritableDirectory',
    'WritableFilePath',
    'argument',
    'cli',
    'find_app',
    'option',
]

argument = click.argument
option = click.option

LOOP_CHOICES = ('aio', 'gevent', 'eventlet', 'uvloop')
DEFAULT_LOOP = 'aio'

# XXX For some reason mypy gives strange errors if we import
# this here: probably mypy bug.
#   python -m mypy faust
#   faust/cli/base.pyfaust/__init__.py:117:
#     error: Module 'faust.agents' has no attribute 'Agent'
#   faust/__init__.py:119:
#     error: Module 'faust.channels' has no attribute 'ChannelT'
#    faust/__init__.py:119:
#       error: Module 'faust.channels' has no attribute 'EventT'
#   make: *** [typecheck] Error 1
faust_version = symbol_by_name('faust:__version__')


class TCPPort(click.IntRange):
    """CLI option: TCP Port (integer in range 1 - 65535)."""

    name = 'range[1-65535]'

    def __init__(self) -> None:
        super().__init__(1, 65535)


WritableDirectory = click.Path(
    exists=False,      # create if needed,
    file_okay=False,   # must be directory,
    dir_okay=True,     # not file;
    writable=True,     # and read/write access.
    readable=True,     #
)

WritableFilePath = click.Path(
    exists=False,       # create if needed,
    file_okay=True,     # must be file,
    dir_okay=False,     # not directory;
    writable=True,      # and read/write access.
    readable=True,      #
)

builtin_options: Sequence[Callable] = [
    click.version_option(version=f'Faust {faust_version}'),
    option('--app', '-A',
           help='Path of Faust application to use, or the name of a module.'),
    option('--quiet/--no-quiet', '-q', default=False,
           help='Silence output to <stdout>/<stderr>.'),
    option('--debug/--no-debug', default=DEBUG,
           help='Enable debugging output, and the blocking detector.'),
    option('--no_color/--color', default=False,
           help='Enable colors in output.'),
    option('--workdir', '-W',
           default=WORKDIR,
           type=WritableDirectory,
           help='Working directory to change to after start.'),
    option('--datadir', '-D',
           default=DATADIR,
           type=WritableDirectory,
           help='Directory to keep application state.'),
    option('--json', default=False, is_flag=True,
           help='Return output in machine-readable JSON format'),
    option('--loop', '-L',
           default=DEFAULT_LOOP,
           type=click.Choice(LOOP_CHOICES),
           help='Event loop implementation to use.'),
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
        >>> # to find an attribute name (.app) in that module.
        >>> # Example below is the same as importing::
        >>> #    from examples.simple import app
        >>> find_app('examples.simple')

        >>> # If you want an attribute other than .app you can
        >>> # use : to separate module and attribute.
        >>> # Examples below is the same as importing::
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
        val = found
    return prepare_app(val, app)


def prepare_app(app: AppT, name: Optional[str]) -> AppT:
    app.finalize()
    if app.conf.origin is None:
        app.conf.origin = name
    if app.conf.autodiscover:
        app.discover()

    # Hack to fix cProfile support.
    main = sys.modules.get('__main__')
    if main is not None and 'cProfile.py' in getattr(main, '__file__', ''):
        from ..models import registry
        registry.update({
            (app.conf.origin or '') + k[8:]: v
            for k, v in registry.items()
            if k.startswith('cProfile.')
        })
    return app


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
        workdir = self._extract_param(argv, '-W', '--workdir')
        if workdir:
            os.chdir(Path(workdir).absolute())
        appstr = self._extract_param(argv, '-A', '--app')
        if appstr is not None:
            find_app(appstr)

    def _extract_param(self,
                       argv: Sequence[str],
                       shortopt: str,
                       longopt: str) -> Optional[str]:
        for i, arg in enumerate(argv):
            if arg == shortopt:
                try:
                    return argv[i + 1]
                except IndexError:
                    raise click.UsageError(f'Missing argument for {shortopt}')
            elif arg.startswith(longopt):
                if '=' in arg:
                    _, _, value = arg.partition('=')
                    return value
                else:
                    try:
                        return argv[i + 1]
                    except IndexError:
                        raise click.UsageError(
                            f'Missing argument for {longopt}')
        return None

    @no_type_check  # mypy bugs out on this
    def make_context(self,
                     info_name: str,
                     args: str,
                     app: AppT = None,
                     parent: click.Context = None,
                     **extra: Any) -> click.Context:
        ctx = super().make_context(info_name, args, **extra)
        self._maybe_import_app()
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
        no_color: bool,
        loop: str) -> None:
    """Faust command-line interface."""
    ctx.obj = {
        'app': app,
        'quiet': quiet,
        'debug': debug,
        'workdir': workdir,
        'datadir': datadir,
        'json': json,
        'no_color': no_color,
        'loop': loop,
    }
    if workdir:
        os.environ['F_WORKDIR'] = workdir
        # XXX I'm not sure this is the best place to chdir [ask]
        os.chdir(Path(workdir).absolute())
    if datadir:
        # This is the only way we can set the datadir for App.__init__,
        # so that default values will have the right path prefix.
        # WARNING: Note that the faust.app module *MUST not* have
        # been imported before setting the envvar.
        os.environ['F_DATADIR'] = datadir
    if not no_color and terminal.isatty(sys.stdout):
        enable_all_colors()
    else:
        disable_all_colors()
    if json:
        disable_all_colors()


class Command(abc.ABC):
    """Base class for subcommands."""

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
    no_color: bool

    builtin_options: List = builtin_options
    options: Optional[List] = None

    args: Tuple
    kwargs: Dict
    prog_name: str = ''

    @classmethod
    def as_click_command(cls) -> Callable:
        # This is what actually registers the commands into the
        # :pypi:`click` command-line interface (the ``def cli`` main above).
        # __init_subclass__ calls this for the side effect of being
        # registered as a `faust` subcommand.
        @click.pass_context
        @wraps(cls)
        def _inner(*args: Any, **kwargs: Any) -> Callable:
            cmd = cls(*args, **kwargs)  # type: ignore
            return cmd()

        return _apply_options(cls.options or [])(
            cli.command(help=cls.__doc__)(_inner))

    def __init_subclass__(self, *args: Any, **kwargs: Any) -> None:
        if self.abstract:
            # sets the class attribute, so next time
            # Command is inherited from it will have abstract=False,
            # unless you set the attribute again in that subclass::
            #   class MyAbstractCommand(Command):
            #       abstract: ClassVar[bool] = True
            #
            #   class x(MyAbstractCommand):
            #       async def run(self) -> None:
            #           print('just here to experience this execution')
            self.abstract = False
        else:
            self._click = self.as_click_command()

        # This hack creates the Command.parse method used to parse
        # command-line arguments in sys.argv and returns a dict.
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
        self.debug = self.ctx.obj['debug']
        self.quiet = self.ctx.obj['quiet']
        self.workdir = self.ctx.obj['workdir']
        self.datadir = self.ctx.obj['datadir']
        self.json = self.ctx.obj['json']
        self.no_color = self.ctx.obj['no_color']
        self.args = args
        self.kwargs = kwargs
        self.prog_name = self.ctx.find_root().command_path
        self.signal_handlers = {
            signal.SIGINT: self.on_sigint,
            signal.SIGTERM: self.on_sigterm,
        }

    @no_type_check   # Subclasses can omit *args, **kwargs in signature.
    async def run(self, *args: Any, **kwargs: Any) -> Any:
        # NOTE: If you override __call__ below, you have a non-async command.
        # This is used by .worker to call the
        # Worker.execute_from_commandline() method.
        ...

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_event_loop()
        self.install_signal_handlers(loop)
        args = self.args + args
        kwargs = {**self.kwargs, **kwargs}
        return loop.run_until_complete(self.run(*args, **kwargs))

    def install_signal_handlers(self,
                                loop: asyncio.AbstractEventLoop) -> None:
        for signum, handler in self.signal_handlers.items():
            loop.add_signal_handler(signum, handler)

    def on_sigint(self) -> None:
        self.on_signal_default_handler(signal.SIGINT)

    def on_sigterm(self) -> None:
        self.on_signal_default_handler(signal.SIGTERM)

    def on_signal_default_handler(self, signal: int) -> None:
        raise KeyboardInterrupt()

    def tabulate(self,
                 data: terminal.TableDataT,
                 headers: Sequence[str] = None,
                 wrap_last_row: bool = True,
                 title: str = '',
                 title_color: str = 'blue',
                 **kwargs: Any) -> str:
        """Create an ANSI representation of a table of two-row tuples.

        See Also:
            Keyword arguments are forwarded to
            :class:`terminaltables.SingleTable`

        Note:
            If the :option:`--json <faust --json>` option is enabled
            this returns json instead.
        """
        if self.json:
            return self._tabulate_json(data, headers=headers)
        if headers:
            data = [headers] + list(data)
        title = self.bold(self.color(title_color, title))
        table = self.table(data, title=title, **kwargs)
        if wrap_last_row:
            # slow, but not big data
            data = [
                list(item[:-1]) + [self._table_wrap(table, item[-1])]
                for item in data
            ]
        return table.table

    def _tabulate_json(self,
                       data: terminal.TableDataT,
                       headers: Sequence[str] = None) -> str:
        if headers:
            return json.dumps([dict(zip(headers, row)) for row in data])
        return json.dumps(data)

    def table(self,
              data: terminal.TableDataT,
              title: str = '',
              **kwargs: Any) -> terminal.Table:
        """Format table data as ANSI/ASCII table."""
        return terminal.table(data, title=title, target=sys.stdout, **kwargs)

    def color(self, name: str, text: str) -> str:
        """Return text having a certain color by name.

        Examples::
            >>> self.color('blue', 'text_to_color')
            >>> self.color('hiblue', text_to_color')

        See Also:
            :pypi:`colorclass`: for a list of available colors.
        """
        return Color(f'{{{name}}}{text}{{/{name}}}')

    def dark(self, text: str) -> str:
        """Return cursor text."""
        return self.color('autoblack', text)

    def bold(self, text: str) -> str:
        """Return text in bold."""
        return self.color('b', text)

    def bold_tail(self, text: str, *, sep: str = '.') -> str:
        """Put bold emphasis on the last part of a foo.bar.baz string."""
        head, fsep, tail = text.rpartition(sep)
        return fsep.join([head, self.bold(tail)])

    def _table_wrap(self, table: terminal.Table, text: str) -> str:
        max_width = max(table.column_max_width(1), 10)
        return '\n'.join(wrap(text, max_width))

    def say(self, *args: Any, **kwargs: Any) -> None:
        """Print something to stdout (or use ``file=stderr`` kwarg).

        Note:
            Does not do anything if the :option:`--quiet <faust --quiet>`
            option is enabled.
        """
        if not self.quiet:
            echo(*args, **kwargs)

    def carp(self, s: Any, **kwargs: Any) -> None:
        """Print something to stdout (or use ``file=stderr`` kwargs).

        Note:
            Does not do anything if the :option:`--debug <faust --debug>`
            option is enabled.
        """
        if self.debug:
            print(f'#-- {s}', **kwargs)

    def dumps(self, obj: Any) -> str:
        return json.dumps(obj)


class AppCommand(Command):
    """Command that takes ``-A app`` as argument."""

    abstract: ClassVar[bool] = True

    app: AppT

    require_app = True

    #: The :term:`codec` used to serialize keys.
    #: Taken from instance parameters or :attr:`@key_serializer`.
    key_serializer: CodecArg

    #: The :term:`codec` used to serialize values.
    #: Taken from instance parameters or :attr:`@value_serializer`.
    value_serialier: CodecArg

    @classmethod
    def from_handler(
            cls,
            *options: Any,
            **kwargs: Any) -> Callable[[Callable], Type['AppCommand']]:
        def _inner(fun: Callable[..., Awaitable[Any]]) -> Type['AppCommand']:
            target: Any = fun
            if not inspect.signature(fun).parameters:
                # if it does not take self argument, use staticmethod
                target = staticmethod(fun)

            fields = {
                'run': target,
                '__doc__': fun.__doc__,
                '__name__': fun.__name__,
                '__qualname__': fun.__qualname__,
                '__module__': fun.__module__,
                '__wrapped__': fun,
                'options': options,
            }
            return type(fun.__name__, (cls,), {**fields, **kwargs})

        return _inner

    def __init__(self,
                 ctx: click.Context,
                 *args: Any,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = None,
                 **kwargs: Any) -> None:
        super().__init__(ctx)

        self.app = getattr(ctx.find_root(), 'app', None)
        if self.app is not None:
            # XXX How to find full argv[0] with click?
            origin = self.app.conf.origin
            if sys.argv:
                prog = Path(sys.argv[0]).absolute()
                paths = []
                p = prog.parent
                # find lowermost path, that is a package
                while p:
                    if not (p / '__init__.py').is_file():
                        break
                    paths.append(p)
                    p = p.parent
                package = '.'.join(
                    [p.name for p in paths] + [prog.with_suffix('').name])
                if package.endswith('.__main__'):
                    # when `python -m pkg`: remove .__main__ from pkg.__main__
                    package = package[:-9]
                origin = package
            prepare_app(self.app, origin)
        else:
            appstr = self.ctx.obj['app']
            if appstr:
                self.app = find_app(appstr)
                conf = self.app.conf
                key_serializer = key_serializer or conf.key_serializer
                value_serializer = value_serializer or conf.value_serializer
            else:
                if self.require_app:
                    raise self.UsageError(
                        'Need to specify app using -A parameter')
        self.args = args
        self.kwargs = kwargs
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    def to_key(self, typ: Optional[str], key: str) -> Any:
        """Convert command-line argument string to model (key).

        Arguments:
            typ: The name of the model to create.
            key: The string json of the data to populate it with.

        Notes:
            Uses :attr:`key_serializer` to set the :term:`codec`
            for the key (e.g. ``"json"``), as set by the
            :option:`--key-serializer <faust send --key-serializer>` option.
        """
        return self.to_model(typ, key, self.key_serializer)

    def to_value(self, typ: Optional[str], value: str) -> Any:
        """Convert command-line argument string to model (value).

        Arguments:
            typ: The name of the model to create.
            key: The string json of the data to populate it with.

        Notes:
            Uses :attr:`value_serializer` to set the :term:`codec`
            for the value (e.g. ``"json"``), as set by the
            :option:`--value-serializer <faust send --value-serializer>`
            option.
        """
        return self.to_model(typ, value, self.value_serializer)

    def to_model(self,
                 typ: Optional[str],
                 value: str,
                 serializer: CodecArg) -> Any:
        """Convert command-line argument to model.

        Generic version of :meth:`to_key`/:meth:`to_value`.

        Arguments:
            typ: The name of the model to create.
            key: The string json of the data to populate it with.
            serializer: The argument setting it apart from to_key/to_value
                enables you to specify a custom serializer not mandated
                by :attr:`key_serializer`, and :attr:`value_serializer`.

        Notes:
            Uses :attr:`value_serializer` to set the :term:`codec`
            for the value (e.g. ``"json"``), as set by the
            :option:`--value-serializer <faust send --value-serializer>`
            option.
        """
        if typ:
            model: ModelT = self.import_relative_to_app(typ)
            return model.loads(want_bytes(value), serializer=serializer)
        return want_bytes(value)

    def import_relative_to_app(self, attr: str) -> Any:
        """Import string like "module.Model", or "Model" to model class."""
        try:
            return symbol_by_name(attr)
        except ImportError as original_exc:
            if not self.app.conf.origin:
                raise
            root, _, _ = self.app.conf.origin.partition(':')
            try:
                return symbol_by_name(f'{root}.models.{attr}')
            except ImportError:
                try:
                    return symbol_by_name(f'{root}.{attr}')
                except ImportError:
                    raise original_exc from original_exc

    def to_topic(self, entity: str) -> Any:
        """Convert topic name given on command-line to ``app.topic()``."""
        if not entity:
            raise self.UsageError('Missing topic/@agent name')
        if entity.startswith('@'):
            # actor prefix: e.g. `faust send @myactorname`
            return self.import_relative_to_app(entity[1:])
        return self.app.topic(entity)

    def abbreviate_fqdn(self, name: str, *, prefix: str = '') -> str:
        """Abbreviate fully-qualified Python name, by removing origin.

        ``app.conf.origin`` is the package where the app is defined,
        so if this is ``examples.simple`` it returns the truncated::

            >>> app.conf.origin
            'examples.simple'
            >>> abbr_fqdn(app.conf.origin,
            ...           'examples.simple.Withdrawal',
            ...           prefix='[...]')
            '[...]Withdrawal'

        but if the package is not part of origin it provides the full path::

            >>> abbr_fqdn(app.conf.origin,
            ...           'examples.other.Foo', prefix='[...]')
            'examples.other.foo'
        """
        return text.abbr_fqdn(self.app.conf.origin, name, prefix=prefix)
