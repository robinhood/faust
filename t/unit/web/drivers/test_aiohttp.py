import asyncio
import pytest
from aiohttp.web import Application
from faust.web import base
from faust.web.drivers.aiohttp import ServerThread, Web
from mode.utils.mocks import AsyncMock, Mock, patch


@pytest.fixture
def web(*, app):
    return Web(app, port=6066, bind='localhost')


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
        thread.web.start_server.assert_called_once_with(thread.loop)

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

        thread.web.stop_server.assert_called_once_with(thread.loop)


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
        with patch('faust.web.drivers.aiohttp.ServerThread') as ServerThread:
            await web.on_start()
            ServerThread.assert_called_once_with(
                web, loop=web.loop, beacon=web.beacon)
            assert web._thread is ServerThread()
            web.add_dependency.assert_called_once_with(web._thread)

    @pytest.mark.asyncio
    async def test_start_server(self, *, web):
        loop = Mock(
            name='loop',
            autospec=asyncio.AbstractEventLoop,
            create_server=AsyncMock(),
        )
        web._app = Mock(name='_app', autospec=Application)
        await web.start_server(loop)

        web._app.make_handler.assert_called_once_with()
        assert web._handler is web._app.make_handler()
        assert web._srv is loop.create_server.coro()
        loop.create_server.asssert_called_once_with(
            web._handler, web.bind, web.port)

    @pytest.mark.asyncio
    async def test_stop_server(self, *, web):
        web._stop_server = AsyncMock(name='_stop_server')
        web._shutdown_webapp = AsyncMock(name='_shutdown_webapp')
        web._shutdown_handler = AsyncMock(name='_shutdown_handler')
        web._cleanup_app = AsyncMock(name='_cleanup_app')

        await web.stop_server(
            Mock(name='loop', autospec=asyncio.AbstractEventLoop))
        web._stop_server.assert_called_once_with()
        web._shutdown_webapp.assert_called_once_with()
        web._shutdown_handler.assert_called_once_with()
        web._cleanup_app.assert_called_once_with()

    @pytest.mark.asyncio
    async def test__stop_server(self, *, web):
        web._srv = None
        await web._stop_server()
        web._srv = Mock(name='_srv', wait_closed=AsyncMock())
        await web._stop_server()
        web._srv.close.assert_called_once_with()
        web._srv.wait_closed.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_shutdown_webapp(self, *, web):
        web._app = None
        await web._shutdown_webapp()
        web._app = Mock(
            name='_app',
            autospec=Application,
            shutdown=AsyncMock(),
        )
        await web._shutdown_webapp()
        web._app.shutdown.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_shutdown_handler(self, *, web):
        web._handler = None
        await web._shutdown_handler()
        web._handler = Mock(name='_handler', shutdown=AsyncMock())
        await web._shutdown_handler()
        web._handler.shutdown.assert_called_with(web.handler_shutdown_timeout)

    @pytest.mark.asyncio
    async def test_cleanup_app(self, *, web):
        web._app = None
        await web._cleanup_app()
        web._app = Mock(
            name='_app',
            autospec=Application,
            cleanup=AsyncMock(),
        )
        await web._cleanup_app()
        web._app.cleanup.assert_called_once_with()
