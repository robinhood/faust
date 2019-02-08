import asyncio
import sys
from pathlib import Path

import pytest
from aiohttp.web import Application
from mode.utils.mocks import AsyncMock, Mock, patch
from yarl import URL

from faust.web import base
from faust.web.drivers.aiohttp import Server, ServerThread

if sys.platform == 'win32':
    DATAPATH = r'\\opt\\data'
else:
    DATAPATH = '/opt/data'


@pytest.fixture
def thread(*, web):
    return ServerThread(web)


@pytest.fixture
def server(*, web):
    return Server(web)


class test_ServerThread:

    def test_constructor(self, *, thread, web):
        assert thread.web is web

    @pytest.mark.asyncio
    async def test_on_start(self, *, thread):
        thread.web.start_server = AsyncMock(name='web.start_server')
        await thread.on_start()

        thread.web.start_server.assert_called_once()

    @pytest.mark.asyncio
    async def test_crash(self, *, thread):
        thread._thread_running = asyncio.Future()
        exc = RuntimeError()
        await thread.crash(exc)
        assert thread._thread_running.exception() is exc

        thread._thread_running = None
        await thread.crash(exc)

    @pytest.mark.asyncio
    async def test_on_thread_start(self, *, thread):
        thread.web = Mock(
            name='web',
            autospec=base.Web,
            stop_server=AsyncMock(),
        )
        await thread.on_thread_stop()

        thread.web.stop_server.assert_called_once()


class test_Server:

    def test_constructor(self, *, server, web):
        assert server.web is web

    @pytest.mark.asyncio
    async def test_on_start(self, *, server, web):
        web.start_server = AsyncMock()
        await server.on_start()
        web.start_server.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_stop(self, *, server, web):
        web.stop_server = AsyncMock()
        await server.on_stop()
        web.stop_server.assert_called_once_with()


class test_Web:

    def test_text(self, *, web):
        with patch('faust.web.drivers.aiohttp.Response') as Response:
            resp = web.text('foo', content_type='app/json', status=303)
            Response.assert_called_once_with(
                content_type='app/json', status=303, text='foo')
            assert resp is Response()

    def test_html(self, *, web):
        with patch('faust.web.drivers.aiohttp.Response') as Response:
            resp = web.html('foo', status=303)
            Response.assert_called_once_with(
                content_type='text/html', status=303, text='foo')
            assert resp is Response()

    def test_bytes(self, *, web):
        with patch('faust.web.drivers.aiohttp.Response') as Response:
            resp = web.bytes(b'foo', content_type='app/json', status=303)
            Response.assert_called_once_with(
                body=b'foo', content_type='app/json', status=303)
            assert resp is Response()

    @pytest.mark.asyncio
    async def test_on_start(self, *, web):
        web.add_dependency = Mock(name='add_dependency')
        web.app.conf.web_in_thread = True
        with patch('faust.web.drivers.aiohttp.ServerThread') as ServerThread:
            await web.on_start()
            ServerThread.assert_called_once_with(
                web, loop=web.loop, beacon=web.beacon)
            assert web._thread is ServerThread()
            web.add_dependency.assert_called_once_with(web._thread)

    @pytest.mark.asyncio
    async def test_wsgi(self, *, web):
        web.init_server = Mock(name='init_server')
        assert await web.wsgi() is web.web_app
        web.init_server.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_start_server(self, *, app, web):
        web.web_app = Mock(name='web.web_app', autospec=Application)
        web._runner = Mock(name='runner', setup=AsyncMock())
        web._create_site = Mock(
            name='create_site',
            return_value=Mock(
                start=AsyncMock(),
            ),
        )

        await web.start_server()

        web._runner.setup.assert_called_once_with()
        web._create_site.assert_called_once_with()
        web._create_site().start.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_stop_server(self, *, web):
        web._cleanup_app = AsyncMock(name='_cleanup_app')

        await web.stop_server()
        web._cleanup_app.assert_called_once_with()
        web._runner = None
        await web.stop_server()

    @pytest.mark.asyncio
    async def test_cleanup_app(self, *, web):
        web.web_app = None
        await web._cleanup_app()
        web.web_app = Mock(
            name='web.web_app',
            autospec=Application,
            cleanup=AsyncMock(),
        )
        await web._cleanup_app()
        web.web_app.cleanup.assert_called_once_with()

    def test_add_static(self, *, web):
        web.web_app = Mock(name='web.web_app', autospec=Application)
        web.add_static('/prefix/', Path(DATAPATH), kw1=3)
        web.web_app.router.add_static.assert_called_once_with(
            '/prefix/', DATAPATH, kw1=3,
        )

    def test__create_site(self, *, web, app):
        web._new_transport = Mock()
        ret = web._create_site()
        web._new_transport.assert_called_once_with(
            app.conf.web_transport.scheme)
        assert ret is web._new_transport()

    def test__new_transport__tcp(self, *, web, app):
        app.conf.web_transport = URL('tcp://')
        with patch('faust.web.drivers.aiohttp.TCPSite') as TCPSite:
            ret = web._create_site()
            TCPSite.assert_called_once_with(
                web._runner,
                app.conf.web_bind,
                app.conf.web_port,
            )
            assert ret is TCPSite()

    def test__new_transport__unix(self, *, web, app):
        app.conf.web_transport = URL('unix:///etc/foobar')
        with patch('faust.web.drivers.aiohttp.UnixSite') as UnixSite:
            ret = web._create_site()
            UnixSite.assert_called_once_with(
                web._runner,
                '/etc/foobar',
            )
            assert ret is UnixSite()

    def test__app__compat_property(self, *, web):
        assert web._app is web.web_app
