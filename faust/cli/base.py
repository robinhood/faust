"""Command-line programs using :pypi:`click`."""
import abc
import asyncio
import inspect
import io
import os
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
    IO,
    List,
    Mapping,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    no_type_check,
)

import click
from click import echo
from colorclass import Color, disable_all_colors, enable_all_colors
from mode import Service, ServiceT, Worker
from mode.utils import text
from mode.utils.compat import want_bytes
from mode.utils.imports import import_from_cwd, symbol_by_name
from mode.utils.typing import NoReturn
from mode.worker import exiting

from faust.types._env import (
    BLOCKING_TIMEOUT,
    CONSOLE_PORT,
    DATADIR,
    DEBUG,
    WORKDIR,
)
from faust.types import AppT, CodecArg, ModelT
from faust.utils import json
from faust.utils import terminal

from . import params

try:
    import click_completion
except ImportError:
    click_completion = None
else:  # pragma: no cover
    click_completion.init()

__all__ = [
    'AppCommand',
    'Command',
    'argument',
    'cli',
    'find_app',
    'option',
]


def argument(*args: Any, **kwargs: Any) -> Callable[[Any], Any]:
    """Create command-line argument.

    SeeAlso:
        :func:`click.argument`
    """
    return click.argument(*args, **kwargs)


# Our version of click.option enables show_default=True by default.
def option(*option_decls: Any,
           show_default: bool = True,
           **kwargs: Any) -> Callable[[Any], Any]:
    """Create command-line option.

    SeeAlso:
        func:`click.option`
    """
    return click.option(*option_decls, show_default=show_default, **kwargs)


OptionDecorator = Callable[[Any], Any]
OptionSequence = Sequence[OptionDecorator]
OptionList = MutableSequence[OptionDecorator]

LOOP_CHOICES: Sequence[str] = ('aio', 'gevent', 'eventlet', 'uvloop')
DEFAULT_LOOP: str = 'aio'
DEFAULT_LOGLEVEL: str = 'WARN'
LOGLEVELS: Sequence[str] = (
    'CRIT',
    'ERROR',
    'WARN',
    'INFO',
    'DEBUG',
)

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


# Worker options moved from `faust worker` to toplevel
# According to @mitsuhiko the following is "really simple to
# implement through decorators."
# [from https://github.com/pallets/click/issues/108]
class State:
    app: Optional[AppT] = None
    quiet: bool = False
    debug: bool = False
    workdir: Optional[str] = None
    datadir: Optional[str] = None
    json: bool = False
    no_color: bool = False
    loop: Optional[str] = None
    logfile: Optional[str] = None
    loglevel: Optional[int] = None
    blocking_timeout: Optional[float] = None
    console_port: Optional[int] = None


def compat_option(
        *args: Any,
        state_key: str,
        callback: Callable[[click.Context, click.Parameter, Any], Any] = None,
        expose_value: bool = False,
        **kwargs: Any) -> Callable[[Any], click.Parameter]:

    def _callback(ctx: click.Context,  # pragma: no cover
                  param: click.Parameter,
                  value: Any) -> Any:
        state = ctx.ensure_object(State)
        prev_value = getattr(state, state_key, None)
        if prev_value is None and value != param.default:
            setattr(state, state_key, value)
        return callback(ctx, param, value) if callback else value

    return option(
        *args, callback=_callback, expose_value=expose_value, **kwargs)


now_builtin_worker_options: OptionSequence = [
    compat_option(
        '--logfile', '-f',
        state_key='logfile',
        default=None,
        type=params.WritableFilePath,
        help='Path to logfile (default is <stderr>).',
    ),
    compat_option(
        '--loglevel', '-l',
        state_key='loglevel',
        default=DEFAULT_LOGLEVEL,
        type=params.CaseInsensitiveChoice(LOGLEVELS),
        help='Logging level to use.',
    ),
    compat_option(
        '--blocking-timeout',
        state_key='blocking_timeout',
        default=BLOCKING_TIMEOUT,
        type=float,
        help='when --debug: Blocking detector timeout.',
    ),
    compat_option(
        '--console-port',
        state_key='console_port',
        default=CONSOLE_PORT,
        type=params.TCPPort(),
        help='when --debug: Port to run debugger console on.',
    ),
]

core_options: OptionSequence = [
    click.version_option(version=f'Faust {faust_version}'),
    option('--app', '-A',
           help='Path of Faust application to use, or the name of a module.'),
    option('--quiet/--no-quiet', '-q', default=False,
           help='Silence output to <stdout>/<stderr>.'),
    option('--debug/--no-debug', default=DEBUG,
           help='Enable debugging output, and the blocking detector.'),
    option('--no-color/--color', '--no_color/--color', default=False,
           help='Enable colors in output.'),
    option('--workdir', '-W',
           default=WORKDIR,
           type=params.WritableDirectory,
           help='Working directory to change to after start.'),
    option('--datadir', '-D',
           default=DATADIR,
           type=params.WritableDirectory,
           help='Directory to keep application state.'),
    option('--json', default=False, is_flag=True,
           help='Return output in machine-readable JSON format'),
    option('--loop', '-L',
           default=DEFAULT_LOOP,
           type=click.Choice(LOOP_CHOICES),
           help='Event loop implementation to use.'),
]

builtin_options = (cast(List, core_options) +
                   cast(List, now_builtin_worker_options))


class _FaustRootContextT(click.Context):
    # used for typing only
    app: Optional[AppT]
    stdout: Optional[IO]
    stderr: Optional[IO]
    side_effects: bool


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
    if app.conf._origin is None:
        app.conf._origin = name
    app.worker_init()
    if app.conf.autodiscover:
        app.discover()

    # Hack to fix cProfile support.
    if 1:  # pragma: no cover
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
def _apply_options(options: OptionSequence) -> OptionDecorator:
    """Add list of ``click.option`` values to click command function."""
    def _inner(fun: OptionDecorator) -> OptionDecorator:
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
                     stdout: IO = None,
                     stderr: IO = None,
                     side_effects: bool = True,
                     **extra: Any) -> click.Context:
        ctx = super().make_context(info_name, args, **extra)
        self._maybe_import_app()
        root = cast(_FaustRootContextT, ctx.find_root())
        root.app = app
        root.stdout = stdout
        root.stderr = stderr
        root.side_effects = side_effects
        return ctx


# This is the thing that app.main(), ``python -m faust -A ...``,
# and ``faust -A ..`` calls (see also faust/__main__.py, and setup.py
# in the git repository (entrypoints).)

@click.group(cls=_Group)
@_apply_options(builtin_options)
@click.pass_context
def cli(*args: Any, **kwargs: Any) -> None:  # pragma: no cover
    """Create :pypi:`click` CLI object."""
    return _prepare_cli(*args, **kwargs)


def _prepare_cli(ctx: click.Context,
                 app: Union[AppT, str],
                 quiet: bool,
                 debug: bool,
                 workdir: str,
                 datadir: str,
                 json: bool,
                 no_color: bool,
                 loop: str) -> None:
    """Faust command-line interface."""
    state = ctx.ensure_object(State)
    state.app = app
    state.quiet = quiet
    state.debug = debug
    state.workdir = workdir
    state.datadir = datadir
    state.json = json
    state.no_color = no_color
    state.loop = loop

    root = cast(_FaustRootContextT, ctx.find_root())
    if root.side_effects:
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
    logfile: str
    _loglevel: Optional[str]
    _blocking_timeout: Optional[float]
    _console_port: Optional[int]

    stdout: Optional[IO]
    stderr: Optional[IO]

    daemon: bool = False
    redirect_stdouts: Optional[bool] = None
    redirect_stdouts_level: Optional[int] = None

    builtin_options: OptionSequence = builtin_options
    options: Optional[OptionList] = None

    args: Tuple
    kwargs: Dict
    prog_name: str = ''

    @classmethod
    def as_click_command(cls) -> Callable:  # pragma: no cover
        # This is what actually registers the commands into the
        # :pypi:`click` command-line interface (the ``def cli`` main above).
        # __init_subclass__ calls this for the side effect of being
        # registered as a `faust` subcommand.
        @click.pass_context
        @wraps(cls)
        def _inner(*args: Any, **kwargs: Any) -> NoReturn:
            cmd = cls(*args, **kwargs)
            with exiting():
                cmd()

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
    def _parse(**kwargs: Any) -> Mapping:  # pragma: no cover
        return kwargs

    def __init__(self, ctx: click.Context, *args: Any, **kwargs: Any) -> None:
        self.ctx = ctx
        root = cast(_FaustRootContextT, self.ctx.find_root())
        self.state = ctx.ensure_object(State)
        self.debug = self.state.debug
        self.quiet = self.state.quiet
        self.workdir = self.state.workdir
        self.datadir = self.state.datadir
        self.json = self.state.json
        self.no_color = self.state.no_color
        self.logfile = self.state.logfile
        self.stdout = root.stdout
        self.stderr = root.stderr
        self.args = args
        self.kwargs = kwargs
        self.prog_name = root.command_path

        # compat options must be None for now, since subcommands
        # also can accept them as arguments.
        self._loglevel = self.state.loglevel
        self._blocking_timeout = self.state.blocking_timeout
        self._console_port = self.state.console_port

    @no_type_check   # Subclasses can omit *args, **kwargs in signature.
    async def run(self, *args: Any, **kwargs: Any) -> Any:
        # NOTE: If you override __call__ below, you have a non-async command.
        # This is used by .worker to call the
        # Worker.execute_from_commandline() method.
        ...

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        try:
            await self.run(*args, **kwargs)
        finally:
            await self.on_stop()

    async def on_stop(self) -> None:
        ...

    def __call__(self, *args: Any, **kwargs: Any) -> NoReturn:
        self.run_using_worker(*args, **kwargs)

    def run_using_worker(self, *args: Any, **kwargs: Any) -> NoReturn:
        loop = asyncio.get_event_loop()
        args = self.args + args
        kwargs = {**self.kwargs, **kwargs}
        service = self.as_service(loop, *args, **kwargs)
        worker = self.worker_for_service(service, loop)
        self.on_worker_created(worker)
        raise worker.execute_from_commandline()

    def on_worker_created(self, worker: Worker) -> None:
        ...

    def as_service(self,
                   loop: asyncio.AbstractEventLoop,
                   *args: Any, **kwargs: Any) -> Service:
        return Service.from_awaitable(
            self.execute(*args, **kwargs),
            name=type(self).__name__,
            loop=loop or asyncio.get_event_loop())

    def worker_for_service(self,
                           service: ServiceT,
                           loop: asyncio.AbstractEventLoop = None) -> Worker:
        return self._Worker(
            service,
            debug=self.debug,
            quiet=self.quiet,
            stdout=self.stdout,
            stderr=self.stderr,
            loglevel=self.loglevel,
            logfile=self.logfile,
            blocking_timeout=self.blocking_timeout,
            console_port=self.console_port,
            redirect_stdouts=self.redirect_stdouts,
            redirect_stdouts_level=self.redirect_stdouts_level,
            loop=loop or asyncio.get_event_loop(),
            daemon=self.daemon,
        )

    @property
    def _Worker(self) -> Type[Worker]:
        return Worker

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

    def say(self, message: str,
            file: IO = None,
            err: IO = None,
            **kwargs: Any) -> None:
        """Print something to stdout (or use ``file=stderr`` kwarg).

        Note:
            Does not do anything if the :option:`--quiet <faust --quiet>`
            option is enabled.
        """
        if not self.quiet:
            echo(message,
                 file=file or self.stdout,
                 # XXX mypy thinks this should be bool(?!)
                 err=cast(bool, err or self.stderr),
                 **kwargs)

    def carp(self, s: Any, **kwargs: Any) -> None:
        """Print something to stdout (or use ``file=stderr`` kwargs).

        Note:
            Does not do anything if the :option:`--debug <faust --debug>`
            option is enabled.
        """
        if self.debug:
            self.say(f'#-- {s}', **kwargs)

    def dumps(self, obj: Any) -> str:
        return json.dumps(obj)

    @property
    def loglevel(self) -> str:
        return self._loglevel or DEFAULT_LOGLEVEL

    @loglevel.setter
    def loglevel(self, level: str) -> None:
        self._loglevel = level

    @property
    def blocking_timeout(self) -> float:
        return self._blocking_timeout or BLOCKING_TIMEOUT

    @blocking_timeout.setter
    def blocking_timeout(self, timeout: float) -> None:
        self._blocking_timeout = timeout

    @property
    def console_port(self) -> int:
        return self._console_port or CONSOLE_PORT

    @console_port.setter
    def console_port(self, port: int) -> None:
        self._console_port = port


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

        self.app = self._finalize_app(
            getattr(ctx.find_root(), 'app', None))
        self.args = args
        self.kwargs = kwargs
        self.key_serializer = key_serializer or self.app.conf.key_serializer
        self.value_serializer = (
            value_serializer or self.app.conf.value_serializer)

    def _finalize_app(self, app: AppT) -> AppT:
        if app is not None:
            return self._finalize_concrete_app(app)
        else:
            return self._app_from_str(self.state.app)

    def _app_from_str(self, appstr: str = None) -> Optional[AppT]:
        if appstr:
            return find_app(appstr)
        else:
            if self.require_app:
                raise self.UsageError(
                    'Need to specify app using -A parameter')
            return None

    def _finalize_concrete_app(self, app: AppT) -> AppT:
        app.finalize()
        # XXX How to find full argv[0] with click?
        origin = app.conf.origin
        if sys.argv:
            origin = self._detect_main_package(sys.argv)
        return prepare_app(app, origin)

    def _detect_main_package(self, argv: List[str]) -> str:  # pragma: no cover
        prog = Path(argv[0]).absolute()
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
        return package

    async def on_stop(self) -> None:
        app = self.app
        # If command started the producer, we should also stop that
        #   - this will flush any buffers before exiting.
        if app._producer is not None and app._producer.started:
            await app._producer.stop()
        # If command started the app, we should stop it.
        #   - could have app.client_only, or app.producer_only set.
        if app.started:
            await app.stop()
        if app._http_client is not None:
            await app._maybe_close_http_client()

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


def call_command(command: str,
                 args: List[str] = None,
                 stdout: IO = None,
                 stderr: IO = None,
                 side_effects: bool = False,
                 **kwargs: Any) -> Tuple[int, IO, IO]:
    exitcode: int = 0
    if stdout is None:
        stdout = io.StringIO()
    if stderr is None:
        stderr = io.StringIO()
    try:
        cli(args=[command] + (args or []),
            side_effects=side_effects,
            stdout=stdout,
            stderr=stderr,
            **kwargs)
    except SystemExit as exc:
        exitcode = exc.code
    return exitcode, stdout, stderr
