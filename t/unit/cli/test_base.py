import io
import json
import os
import sys
import types
from pathlib import Path
import click
import pytest
from faust.cli import AppCommand, Command, call_command
from faust.cli.base import (
    DEFAULT_LOGLEVEL,
    _Group,
    _prepare_cli,
    compat_option,
    find_app,
)
from faust.types._env import BLOCKING_TIMEOUT, CONSOLE_PORT
from mode import Worker
from mode.utils.mocks import AsyncMock, Mock, call, patch


def test_call_command():
    with patch('faust.cli.base.cli') as cli:
        cli.side_effect = SystemExit(3)
        exitcode, stdout, stderr = call_command('foo', ['x', 'y'])
        cli.assert_called_once_with(
            args=['foo', 'x', 'y'],
            side_effects=False,
            stdout=stdout,
            stderr=stderr,
        )
        assert exitcode == 3


def test_call_command__no_exit():
    with patch('faust.cli.base.cli') as cli:
        exitcode, stdout, stderr = call_command('foo', ['x', 'y'])
        cli.assert_called_once_with(
            args=['foo', 'x', 'y'],
            side_effects=False,
            stdout=stdout,
            stderr=stderr,
        )
        assert exitcode == 0


def test_call_command__custom_ins():
    o_out = io.StringIO()
    o_err = io.StringIO()
    with patch('faust.cli.base.cli') as cli:
        exitcode, stdout, stderr = call_command(
            'foo', ['x', 'y'],
            stdout=o_out,
            stderr=o_err,
        )
        cli.assert_called_once_with(
            args=['foo', 'x', 'y'],
            side_effects=False,
            stdout=stdout,
            stderr=stderr,
        )
        assert exitcode == 0
        assert stdout is o_out
        assert stderr is o_err


def test_compat_option():
    option = compat_option('--foo', default=1, state_key='foo')
    ctx = Mock(name='ctx')
    param = Mock(name='param')
    state = ctx.ensure_object.return_value
    state.foo = 33
    print(dir(option(ctx)))
    option(ctx)._callback(ctx, param, None) == 33
    option(ctx)._callback(ctx, param, 44) == 44
    state.foo = None
    option(ctx)._callback(ctx, param, 44) == 44


def test_find_app():
    imp = Mock(name='imp')
    symbol_by_name = Mock(name='symbol_by_name')
    with patch('faust.cli.base.prepare_app') as prepare_app:
        res = find_app('foo', imp=imp, symbol_by_name=symbol_by_name)
        assert res is prepare_app.return_value
        symbol_by_name.assert_called_once_with('foo', imp=imp)
        prepare_app.assert_called_once_with(
            symbol_by_name.return_value, 'foo')


def test_find_app__attribute_error():
    imp = Mock(name='imp')
    symbol_by_name = Mock(name='symbol_by_name')
    symbol_by_name.side_effect = AttributeError()
    with patch('faust.cli.base.prepare_app') as prepare_app:
        res = find_app('foo', imp=imp, symbol_by_name=symbol_by_name)
        assert res is prepare_app.return_value
        symbol_by_name.assert_called_once_with('foo', imp=imp)
        imp.assert_called_once_with('foo')
        prepare_app.assert_called_once_with(
            imp.return_value, 'foo')


def test_find_app__app_is_module():
    imp = Mock(name='imp')
    symbol_by_name = Mock(name='symbol_by_name')
    symbol_by_name.side_effect = AttributeError()
    imp.return_value = types.ModuleType('foo')
    imp.return_value.app = types.ModuleType('foo.app')
    with pytest.raises(AttributeError):
        find_app('foo', imp=imp, symbol_by_name=symbol_by_name)


def test_find_app__app_is_module_but_has_app():
    imp = Mock(name='imp')
    symbol_by_name = Mock(name='symbol_by_name')
    symbol_by_name.side_effect = AttributeError()
    imp.return_value = types.ModuleType('foo')
    imp.return_value.app = Mock(name='app')
    with patch('faust.cli.base.prepare_app') as prepare_app:
        find_app('foo', imp=imp, symbol_by_name=symbol_by_name)
        prepare_app.assert_called_once_with(imp.return_value.app, 'foo')


class test_Group:

    @pytest.fixture()
    def group(self):
        return _Group()

    def test_get_help(self, *, group):
        ctx = group.make_context('prog', ['x'])
        group._maybe_import_app = Mock()
        group.get_help(ctx)
        group._maybe_import_app.assert_called_once_with()

    def test_get_usage(self, *, group):
        ctx = group.make_context('prog', ['x'])
        group._maybe_import_app = Mock()
        group.get_usage(ctx)
        group._maybe_import_app.assert_called_once_with()

    @pytest.mark.parametrize('argv,expected_chdir,expected_app', [
        (['--foo', '-W', '/foo'], '/foo', None),
        (['--foo', '--workdir', '/foo'], '/foo', None),
        (['--foo', '--workdir=/foo'], '/foo', None),
        (['--foo', '-A', 'foo'], None, 'foo'),
        (['--foo', '--app', 'foo'], None, 'foo'),
        (['--foo', '--app=foo'], None, 'foo'),
        (['--foo', '--app=foo', '--workdir', '/foo'], '/foo', 'foo'),
        ([], None, None),
    ])
    def test_maybe_import_app(self, argv, expected_chdir, expected_app, *,
                              group):
        with patch('os.chdir') as chdir:
            with patch('faust.cli.base.find_app') as find_app:
                group._maybe_import_app(argv)
                if expected_chdir:
                    chdir.assert_called_once_with(
                        Path(expected_chdir).absolute())
                else:
                    chdir.assert_not_called()
                if expected_app:
                    find_app.assert_called_once_with(expected_app)
                else:
                    find_app.assert_not_called()

    def test_maybe_import_app__missing_argument(self, *, group):
        with pytest.raises(click.UsageError):
            group._maybe_import_app(['--foo', '-A'])
        with pytest.raises(click.UsageError):
            group._maybe_import_app(['--foo', '--app'])


def test__prepare_cli():
    ctx = Mock(name='context')
    state = ctx.ensure_object.return_value = Mock(name='state')
    root = ctx.find_root.return_value = Mock(name='root')
    root.side_effects = False
    app = Mock(name='app')
    try:
        _prepare_cli(
            ctx,
            app=app,
            quiet=False,
            debug=True,
            workdir='/foo',
            datadir='/data',
            json=True,
            no_color=False,
            loop='foo',
        )

        assert state.app is app
        assert not state.quiet
        assert state.debug
        assert state.workdir == '/foo'
        assert state.datadir == '/data'
        assert state.json
        assert not state.no_color
        assert state.loop == 'foo'

        root.side_effects = True
        with patch('os.chdir') as chdir:
            with patch('faust.cli.base.enable_all_colors') as eac:
                with patch('faust.utils.terminal.isatty') as isatty:
                    with patch('faust.cli.base.disable_all_colors') as dac:
                        isatty.return_value = True
                        _prepare_cli(
                            ctx,
                            app=app,
                            quiet=False,
                            debug=True,
                            workdir='/foo',
                            datadir='/data',
                            json=False,
                            no_color=False,
                            loop='foo',
                        )
                        chdir.assert_called_with(Path('/foo').absolute())
                        eac.assert_called_once_with()
                        dac.assert_not_called()
                        _prepare_cli(
                            ctx,
                            app=app,
                            quiet=False,
                            debug=True,
                            workdir='/foo',
                            datadir='/data',
                            json=True,
                            no_color=False,
                            loop='foo',
                        )
                        dac.assert_called_once_with()
                        dac.reset_mock()
                        eac.reset_mock()
                        _prepare_cli(
                            ctx,
                            app=app,
                            quiet=False,
                            debug=True,
                            workdir='/foo',
                            datadir='/data',
                            json=False,
                            no_color=True,
                            loop='foo',
                        )
                        eac.assert_not_called()
                        dac.assert_called_once_with()
                        _prepare_cli(
                            ctx,
                            app=app,
                            quiet=False,
                            debug=True,
                            workdir=None,
                            datadir=None,
                            json=False,
                            no_color=True,
                            loop='foo',
                        )
    finally:
        os.environ.pop('F_DATADIR', None)
        os.environ.pop('F_WORKDIR', None)


class test_Command:

    class TestCommand(Command):
        options = [click.option('--quiet/--no-quiet')]

    @pytest.fixture()
    def ctx(self):
        return Mock(name='ctx')

    @pytest.fixture()
    def command(self, *, ctx):
        return self.TestCommand(ctx=ctx)

    @pytest.mark.asyncio
    async def test_run(self, *, command):
        await command.run()

    @pytest.mark.asyncio
    async def test_execute(self, *, command):
        command.run = AsyncMock()
        command.on_stop = AsyncMock()
        await command.execute(2, kw=3)
        command.run.assert_called_once_with(2, kw=3)
        command.on_stop.assert_called_once_with()

    def test_parse(self, *, command):
        with pytest.raises(click.UsageError):
            assert command.parse(['foo', '--quiet'])['quiet']

    def test__parse(self, *, command):
        assert command._parse

    @pytest.mark.asyncio
    async def test_on_stop(self, *, command):
        await command.on_stop()

    def test__call__(self, *, command):
        command.run_using_worker = Mock()
        command(2, kw=2)
        command.run_using_worker.assert_called_once_with(2, kw=2)

    def test_run_using_worker(self, *, command):
        command.args = (2,)
        command.kwargs = {'kw': 1, 'yw': 6}
        command.as_service = Mock()
        command.worker_for_service = Mock()
        worker = command.worker_for_service.return_value
        worker.execute_from_commandline.side_effect = KeyError()

        with patch('asyncio.get_event_loop') as get_event_loop:
            with pytest.raises(KeyError):
                command.run_using_worker(1, kw=2)
                command.as_service.assert_called_once_with(
                    get_event_loop(), 2, 1, kw=2, yw=6)
                command.worker_for_service.assert_called_once_with(
                    command.worker_for_service.return_value,
                    get_event_loop.return_value,
                )
                worker.execute_from_commandline.assert_called_once_with()

    def test_on_worker_created(self, *, command):
        assert command.on_worker_created(Mock(name='worker')) is None

    def test_as_service(self, *, command):
        loop = Mock()
        command.execute = Mock()
        with patch('faust.cli.base.Service') as Service:
            res = command.as_service(loop, 1, kw=2)
            assert res is Service.from_awaitable.return_value
            Service.from_awaitable.assert_called_once_with(
                command.execute.return_value,
                name=type(command).__name__,
                loop=loop,
            )
            command.execute.assert_called_once_with(1, kw=2)

    def test_worker_for_service(self, *, command):
        with patch('faust.cli.base.Worker') as Worker:
            service = Mock(name='service')
            loop = Mock(name='loop')
            res = command.worker_for_service(service, loop)
            assert res is Worker.return_value
            Worker.assert_called_once_with(
                service,
                debug=command.debug,
                quiet=command.quiet,
                stdout=command.stdout,
                stderr=command.stderr,
                loglevel=command.loglevel,
                logfile=command.logfile,
                blocking_timeout=command.blocking_timeout,
                console_port=command.console_port,
                redirect_stdouts=command.redirect_stdouts,
                redirect_stdouts_level=command.redirect_stdouts_level,
                loop=loop,
                daemon=command.daemon,
            )

    def test__Worker(self, *, command):
        assert command._Worker is Worker

    def test_tabulate__when_text(self, *, command):
        command.json = False
        data = [
            ['A', 'B', 'C'],
            ['D', 'E', 'F'],
        ]
        headers = ['a', 'b', 'c']
        assert command.tabulate(data, headers=headers)
        assert command.tabulate(data, headers=None)
        assert command.tabulate(data, headers=None, wrap_last_row=False)

    def test_tabulate__when_json(self, *, command):
        command.json = True
        data = [
            ['A', 'B', 'C'],
            ['D', 'E', 'F'],
        ]
        headers = ['a', 'b', 'c']
        assert json.loads(command.tabulate(data, headers=headers)) == [
            {'a': 'A',
             'b': 'B',
             'c': 'C'},
            {'a': 'D',
             'b': 'E',
             'c': 'F'},
        ]

    def test_tabulate_json(self, *, command):
        data = [
            ['A', 'B', 'C'],
            ['D', 'E', 'F'],
        ]
        assert json.loads(command._tabulate_json(data, headers=None)) == data

    def test_tabulate_json__headers(self, *, command):
        data = [
            ['A', 'B', 'C'],
            ['D', 'E', 'F'],
        ]
        headers = ['a', 'b', 'c']
        assert json.loads(command._tabulate_json(data, headers=headers)) == [
            {'a': 'A',
             'b': 'B',
             'c': 'C'},
            {'a': 'D',
             'b': 'E',
             'c': 'F'},
        ]

    def test_table(self, *, command):
        with patch('faust.utils.terminal.table') as table:
            data = [['A', 'B', 'C']]
            t = command.table(data, title='foo')
            assert t is table.return_value
            table.assert_called_once_with(
                data, title='foo', target=sys.stdout)

    def test_color(self, *, command):
        assert command.color('blue', 'text')

    def test_dark(self, *, command):
        assert command.dark('text')

    def test_bold(self, *, command):
        assert command.bold('text')

    def test_bold_tail(self, *, command):
        assert command.bold_tail('foo.bar.baz')

    def test_table_wrap(self, *, command):
        table = command.table([['A', 'B', 'C']])
        assert command._table_wrap(table, 'fooawqe' * 100)

    def test_say(self, *, command):
        with patch('faust.cli.base.echo') as echo:
            command.quiet = True
            command.say('foo')
            echo.assert_not_called()

            command.quiet = False
            command.say('foo')
            echo.assert_called_once_with(
                'foo', file=command.stdout, err=command.stderr)

    def test_carp(self, *, command):
        command.say = Mock()
        command.debug = False
        command.carp('foo')
        command.say.assert_not_called()
        command.debug = True
        command.carp('foo')
        command.say.assert_called_once()

    def test_dumps(self, *, command):
        with patch('faust.utils.json.dumps') as dumps:
            obj = Mock(name='obj')
            assert command.dumps(obj) is dumps.return_value
            dumps.assert_called_once_with(obj)

    def test_loglevel(self, *, command, ctx):
        assert command.loglevel == ctx.ensure_object().loglevel
        command.loglevel = 'FOO'
        assert command.loglevel == 'FOO'
        command.loglevel = None
        assert command.loglevel == DEFAULT_LOGLEVEL

    def test_blocking_timeout(self, *, command, ctx):
        assert command.blocking_timeout == ctx.ensure_object().blocking_timeout
        command.blocking_timeout = 32.41
        assert command.blocking_timeout == 32.41
        command.blocking_timeout = None
        assert command.blocking_timeout == BLOCKING_TIMEOUT

    def test_console_port(self, *, command, ctx):
        assert command.console_port == ctx.ensure_object().console_port
        command.console_port = 3241
        assert command.console_port == 3241
        command.console_port = None
        assert command.console_port == CONSOLE_PORT


class test_AppCommand:

    @pytest.fixture()
    def ctx(self):
        return Mock(name='ctx')

    @pytest.fixture()
    def command(self, *, app, ctx):
        return AppCommand(app=app, ctx=ctx)

    def test_finalize_app__str(self, *, command):
        command._app_from_str = Mock()
        command.state.app = 'foo'
        res = command._finalize_app(None)
        assert res is command._app_from_str.return_value
        command._app_from_str.assert_called_once_with('foo')

    def test_finalize_app__concrete(self, *, command, app):
        command._finalize_concrete_app = Mock()
        res = command._finalize_app(app)
        assert res is command._finalize_concrete_app.return_value
        command._finalize_concrete_app.assert_called_once_with(app)

    def test_app_from_str(self, *, command):
        with patch('faust.cli.base.find_app') as find_app:
            res = command._app_from_str('foo')
            assert res is find_app.return_value
            find_app.assert_called_once_with('foo')

    def test_app_from_str__empty(self, *, command):
        command.require_app = False
        assert command._app_from_str(None) is None
        command.require_app = True
        with pytest.raises(command.UsageError):
            command._app_from_str(None)

    def test_finalize_concrete_app(self, *, command, app):
        with patch('sys.argv', []):
            command._finalize_concrete_app(app)

    @pytest.mark.asyncio
    async def test_on_stop(self, *, command, ctx):
        app = ctx.find_root().app
        app._producer = None
        app._http_client = None
        app.started = False
        await command.on_stop()

        app._producer = Mock(stop=AsyncMock())
        app._maybe_close_http_client = AsyncMock()
        app._http_client = Mock()
        app.stop = AsyncMock()
        app.started = True

        await command.on_stop()
        app._producer.stop.assert_called_once_with()
        app.stop.assert_called_once_with()
        app._maybe_close_http_client.assert_called_once_with()

    def test_to_key(self, *, command):
        command.to_model = Mock()
        res = command.to_key('typ', 'key')
        assert res is command.to_model.return_value
        command.to_model.assert_called_once_with(
            'typ', 'key', command.key_serializer)

    def test_to_value(self, *, command):
        command.to_model = Mock()
        res = command.to_value('typ', 'value')
        assert res is command.to_model.return_value
        command.to_model.assert_called_once_with(
            'typ', 'value', command.value_serializer)

    def test_to_model(self, *, command):
        command.import_relative_to_app = Mock()
        res = command.to_model('typ', 'value', 'json')
        command.import_relative_to_app.assert_called_once_with('typ')
        model = command.import_relative_to_app.return_value
        assert res is model.loads.return_value
        model.loads.assert_called_once_with(b'value', serializer='json')

    def test_to_model__bytes(self, *, command):
        assert command.to_model(None, 'value', 'json') == b'value'

    def test_import_relative_to_app(self, *, command, ctx):
        with patch('faust.cli.base.symbol_by_name') as symbol_by_name:
            res = command.import_relative_to_app('foo')
            assert res is symbol_by_name.return_value
            symbol_by_name.assert_called_once_with('foo')

    def test_import_relative_to_app__no_origin(self, *, command, ctx):
        app = ctx.find_root().app
        app.conf.origin = None
        with patch('faust.cli.base.symbol_by_name') as symbol_by_name:
            symbol_by_name.side_effect = ImportError()
            with pytest.raises(ImportError):
                command.import_relative_to_app('foo')

    def test_import_relative_to_app__with_origin(self, *, command, ctx):
        app = ctx.find_root().app
        app.conf.origin = 'root.moo:bar'
        with patch('faust.cli.base.symbol_by_name') as symbol_by_name:
            symbol_by_name.side_effect = ImportError()
            with pytest.raises(ImportError):
                command.import_relative_to_app('foo')
            symbol_by_name.assert_has_calls([
                call('foo'),
                call('root.moo.models.foo'),
                call('root.moo.foo'),
            ])

    def test_import_relative_to_app__with_origin_l1(self, *, command, ctx):
        app = ctx.find_root().app
        app.conf.origin = 'root.moo:bar'
        with patch('faust.cli.base.symbol_by_name') as symbol_by_name:
            symbol_by_name.side_effect = [ImportError(), ImportError(), 'x']
            assert command.import_relative_to_app('foo') == 'x'
            symbol_by_name.assert_has_calls([
                call('foo'),
                call('root.moo.models.foo'),
                call('root.moo.foo'),
            ])

    def test_import_relative_to_app__with_origin_l2(self, *, command, ctx):
        app = ctx.find_root().app
        app.conf.origin = 'root.moo:bar'
        with patch('faust.cli.base.symbol_by_name') as symbol_by_name:
            symbol_by_name.side_effect = [ImportError(), 'x']
            assert command.import_relative_to_app('foo') == 'x'
            symbol_by_name.assert_has_calls([
                call('foo'),
                call('root.moo.models.foo'),
            ])

    def test_to_topic__missing(self, *, command):
        with pytest.raises(command.UsageError):
            command.to_topic(None)

    def test_to_topic__agent_prefix(self, *, command):
        command.import_relative_to_app = Mock()
        res = command.to_topic('@agent')
        assert res is command.import_relative_to_app.return_value
        command.import_relative_to_app.assert_called_once_with('agent')

    def test_to_topic__topic_name(self, *, command, ctx):
        app = ctx.find_root().app
        app.topic = Mock()
        assert command.to_topic('foo') is app.topic.return_value
        app.topic.assert_called_once_with('foo')

    def test_abbreviate_fqdn(self, *, command, ctx):
        with patch('mode.utils.text.abbr_fqdn') as abbr_fqdn:
            ret = command.abbreviate_fqdn('foo.bar.baz', prefix='prefix')
            assert ret is abbr_fqdn.return_value
            abbr_fqdn.assert_called_once_with(
                ctx.find_root().app.conf.origin,
                'foo.bar.baz',
                prefix='prefix',
            )

    def test_from_handler_no_params(self, *, command):
        @command.from_handler()
        async def takes_no_args():
            ...

        @command.from_handler()
        async def takes_self_arg(self):
            ...
