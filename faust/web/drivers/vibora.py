"""Web driver using :pypi:`aiohttp`."""
import asyncio
from functools import partial
from pathlib import Path
from socket import (
    IPPROTO_TCP,
    SOL_SOCKET,
    SO_REUSEADDR,
    SO_REUSEPORT,
    TCP_NODELAY,
    socket,
)
from typing import Any, Callable, Optional, Union, cast

from vibora import Vibora
from vibora.__version__ import __version__ as vibora_version
from vibora.hooks import Events
from vibora.responses import JsonResponse, Response
from vibora.router.router import Route  # noqa
from vibora.server import Request
from mode.threads import ServiceThread
from mode.utils.futures import notify

from faust.types import AppT
from faust.web import base

__all__ = ['Web']

_bytes = bytes


class Worker:

    def __init__(self, app, bind, port, sock=None) -> None:
        self.app = app
        self.bind = bind
        self.port = port
        self.socket = sock
        self._handler = None
        self._srv = None

    async def start_server(self, loop: asyncio.AbstractEventLoop) -> None:
        app = self.app
        if not self.socket:
            self.socket = socket()
            self.socket.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1)
            self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            self.socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
            self.socket.bind(((self.bind, self.port)))

        app.initialize()

        await app.call_hooks(
            Events.BEFORE_SERVER_START,
            components=app.components,
        )

        def _handler(*args, **kwargs):
            return app.handler(*args, **kwargs)

        handler = self._handler = partial(
            _handler,
            app=app,
            loop=loop,
            worker=self,
        )
        ss = self._srv = loop.create_server(
            handler,
            sock=self.socket,
            reuse_port=True,
            backlog=1000,
        )

        await ss
        await app.call_hooks(
            Events.AFTER_SERVER_START,
            components=app.components,
        )

    async def stop_server(self, loop, timeout=30):
        await self.app.call_hooks(
            Events.BEFORE_SERVER_STOP,
            components=self.app.components,
        )

        for connection in self.app.connections.copy():
            connection.stop()

        while timeout:
            all_closed = True
            for connection in self.app.connections:
                if not connection.is_closed():
                    all_closed = False
                    break
            if all_closed:
                break
            timeout -= 1
            await asyncio.sleep(1)

    async def _connect_tcp(self,
                           loop: asyncio.AbstractEventLoop,
                           handler: Any) -> asyncio.AbstractServer:
        server = await loop.create_server(
            handler, self.app.conf.web_bind, self.app.conf.web_port)
        return server


class ServerThread(ServiceThread):
    _port_open: Optional[asyncio.Future] = None

    def __init__(self, web: 'Web', **kwargs: Any) -> None:
        self.web = web
        super().__init__(**kwargs)

    async def start(self) -> None:  # pragma: no cover
        self._port_open = asyncio.Future(loop=self.parent_loop)
        await super().start()
        # thread exceptions do not propagate to the main thread, so we
        # need some way to communicate socket open errors, such as "port in
        # use", back to the parent thread.  The _port_open future is set to
        # an exception state when that happens, and awaiting will propagate
        # the error to the parent thread.
        if not self.should_stop:
            try:
                await self._port_open
            finally:
                self._port_open = None

    async def on_start(self) -> None:
        await self.web.start_server(self.loop)
        notify(self._port_open)  # <- .start() can return now

    async def crash(self, exc: BaseException) -> None:
        if self._port_open and not self._port_open.done():
            self._port_open.set_exception(exc)  # <- .start() will raise
        await super().crash(exc)

    async def on_thread_stop(self) -> None:
        # on_stop() executes in parent thread, on_thread_stop in the thread.
        await self.web.stop_server(self.loop)


class Web(base.Web):
    """Web server and framework implemention using :pypi:`aiohttp`."""

    driver_version = f'vibora={vibora_version}'
    handler_shutdown_timeout: float = 60.0

    #: We serve the web server in a separate thread (and separate event loop).
    _thread: Optional[ServerThread] = None

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(app, **kwargs)
        self.web_app: Vibora = Vibora()
        self.web_app.debug_mode = True
        self._srv: Any = None
        self._handler: Any = None

    async def on_start(self) -> None:
        self.init_server()
        self._thread = ServerThread(
            self,
            loop=self.loop,
            beacon=self.beacon,
        )
        self.add_dependency(self._thread)

    async def wsgi(self) -> Any:
        self.init_server()
        return self.web_app

    def text(self, value: str, *, content_type: str = None,
             status: int = 200) -> base.Response:
        response = Response(
            text=value,
            content_type=content_type,
            status=status,
        )
        return cast(base.Response, response)

    def html(self, value: str, *, status: int = 200) -> base.Response:
        return self.text(value, status=status, content_type='text/html')

    def json(self, value: Any, *, status: int = 200) -> Any:
        return JsonResponse(value, status=status)

    def bytes(self,
              value: _bytes,
              *,
              content_type: str = None,
              status: int = 200) -> base.Response:
        response = Response(
            body=value,
            status=status,
            content_type=content_type,
        )
        return cast(base.Response, response)

    async def read_request_content(self, request: base.Request) -> _bytes:
        return await cast(Request, request).content.read()

    def route(self, pattern: str, handler: Callable) -> None:
        print('ROUTE: %r -> %r' % (pattern, handler))
        # self.web_app.router.add_route(Route(
        #    app=self.web_app,
        #    pattern=pattern.encode(),
        #    handler=self._wrap_into_asyncdef(handler),
        # ))
        print('REVERSE NOW: %r' % (self.web_app.router.reverse_index,))

    def _wrap_into_asyncdef(self, handler: Callable) -> Callable:
        # get rid of pesky "DeprecationWarning: Bare functions are
        # deprecated, use async ones" warnings.
        # The handler is actually a class that defines `async def __call__`
        # but aiohttp doesn't recognize it as such and emits the warning.
        # To avoid that we just wrap it in an `async def` function
        async def _dispatch(request: base.Request) -> base.Response:
            return await handler(request)
        return _dispatch

    def add_static(self,
                   prefix: str,
                   path: Union[Path, str],
                   **kwargs: Any) -> None:
        self.web_app.router.add_static(prefix, str(path), **kwargs)

    def bytes_to_response(self, s: _bytes) -> base.Response:
        status, headers, body = self._bytes_to_response(s)
        response = Response(
            body=body,
            status=status,
            headers=headers,
        )
        return cast(base.Response, response)

    def response_to_bytes(self, response: base.Response) -> _bytes:
        resp = cast(Response, response)
        return self._response_to_bytes(
            resp.status,
            resp.headers,
            resp.body,
        )

    async def start_server(self, loop: asyncio.AbstractEventLoop) -> None:
        worker = self._handler = Worker(
            app=self.web_app,
            bind=self.app.conf.web_bind,
            port=self.app.conf.web_port,
        )
        await worker.start_server(loop)

    async def stop_server(self, loop: asyncio.AbstractEventLoop) -> None:
        await self._shutdown_handler(loop)
        await self._cleanup_app()

    async def _shutdown_handler(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._handler is not None:
            self.log.info('Waiting for handler to shut down')
            await self._handler.stop_server(
                loop, timeout=self.handler_shutdown_timeout)

    async def _cleanup_app(self) -> None:
        if self.web_app is not None:
            self.log.info('Cleanup')
            self.web_app.clean_up()

    @property
    def _app(self) -> Vibora:
        # XXX compat alias
        return self.web_app
