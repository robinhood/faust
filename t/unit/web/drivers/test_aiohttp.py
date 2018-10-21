import asyncio

import pytest
from aiohttp.web import Application, TCPSite, UnixSite
from mode.utils.mocks import AsyncMock, Mock, patch

from faust.web import base
from faust.web.drivers.aiohttp import ServerThread


@pytest.fixture
def thread(*, web):
    return ServerThread(web)


class test_ServerThread:

    def test_constructor(self, *, thread, web):
        assert thread.web is web

    @pytest.mark.asyncio
    async def test_on_start(self, *, thread):
        thread.web.start_server = AsyncMock(name='web.start_server')
        thread._port_open = asyncio.Future()
        await thread.on_start()

        assert thread._port_open.done()
        thread.web.start_server.assert_called_once()

    @pytest.mark.asyncio
    async def test_crash(self, *, thread):
        thread._port_open = asyncio.Future()
        exc = RuntimeError()
        await thread.crash(exc)
        assert thread._port_open.exception() is exc

        thread._port_open = None
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
        serv = web._service
        serv.add_dependency = Mock(name='add_dependency')
        with patch('faust.web.drivers.aiohttp.ServerThread') as ServerThread:
            await serv.on_start()
            ServerThread.assert_called_once_with(
                web, loop=serv.loop, beacon=serv.beacon)
            assert serv._thread is ServerThread()
            serv.add_dependency.assert_called_once_with(serv._thread)

    @pytest.mark.asyncio
    async def test_start_server(self, *, app, web):
        web.web_app = Mock(name='web.web_app', autospec=Application)
        await web.start_server()
        assert isinstance(list(web._runner.sites)[0], TCPSite)

    @pytest.mark.asyncio
    async def test_start_server_unix_socket(self, *, app, web):
        app.conf.web_transport = 'unix:///tmp/server.sock'
        await web.start_server()
        assert isinstance(list(web._runner.sites)[0], UnixSite)

    @pytest.mark.asyncio
    async def test_stop_server(self, *, web):
        web._cleanup_app = AsyncMock(name='_cleanup_app')

        await web.stop_server()
        web._cleanup_app.assert_called_once_with()

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
