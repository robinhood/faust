"""Web driver using :pypi:`aiohttp`."""
import asyncio
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Optional, Union, cast

from aiohttp import __version__ as aiohttp_version
from aiohttp.web import Application, Request, Response, json_response
from mode.threads import ServiceThread

from faust.types import AppT
from faust.web import base
from faust.utils import json as _json

__all__ = ['Web']

_bytes = bytes


class ServerThread(ServiceThread):

    def __init__(self, web: 'Web', **kwargs: Any) -> None:
        self.web = web
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        await self.web.start_server(self.loop)

    async def on_thread_stop(self) -> None:
        # on_stop() executes in parent thread, on_thread_stop in the thread.
        await self.web.stop_server(self.loop)


class Web(base.Web):
    """Web server and framework implemention using :pypi:`aiohttp`."""

    driver_version = f'aiohttp={aiohttp_version}'
    handler_shutdown_timeout: float = 60.0

    #: We serve the web server in a separate thread (and separate event loop).
    _thread: Optional[ServerThread] = None

    _transport_schemes: Mapping[
        str,
        Callable[[asyncio.AbstractEventLoop, Any],
                 Awaitable[asyncio.AbstractServer]],
    ]

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(app, **kwargs)
        self.web_app: Application = Application()
        self._srv: Any = None
        self._handler: Any = None
        self._transport_schemes = {
            'tcp': self._connect_tcp,
            'unix': self._connect_unix,
        }

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
        return json_response(value, status=status, dumps=_json.dumps)

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
        self.web_app.router.add_route(
            '*', pattern, self._wrap_into_asyncdef(handler))

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
        handler = self._handler = self.web_app.make_handler()
        self._srv = await self.create_server(loop, handler)

    async def create_server(self,
                            loop: asyncio.AbstractEventLoop,
                            handler: Any) -> asyncio.AbstractServer:
        transport = self.app.conf.web_transport
        return await self._transport_schemes[transport.scheme](loop, handler)

    async def _connect_tcp(self,
                           loop: asyncio.AbstractEventLoop,
                           handler: Any) -> asyncio.AbstractServer:
        server = await loop.create_server(
            handler, self.app.conf.web_bind, self.app.conf.web_port)
        self.log.info('Serving on %s', self.url)
        return server

    async def _connect_unix(self,
                            loop: asyncio.AbstractEventLoop,
                            handler: Any) -> asyncio.AbstractServer:
        server = await loop.create_unix_server(
            handler, self.app.conf.web_transport.path)
        self.log.info('Serving on %s', self.app.conf.web_transport.path)
        return server

    async def stop_server(self, loop: asyncio.AbstractEventLoop) -> None:
        await self._stop_server()
        await self._shutdown_webapp()
        await self._shutdown_handler()
        await self._cleanup_app()

    async def _stop_server(self) -> None:
        if self._srv is not None:
            self.log.info('Closing server')
            self._srv.close()
            self.log.info('Waiting for server to close handle')
            await self._srv.wait_closed()

    async def _shutdown_webapp(self) -> None:
        if self.web_app is not None:
            self.log.info('Shutting down web application')
            await self.web_app.shutdown()

    async def _shutdown_handler(self) -> None:
        if self._handler is not None:
            self.log.info('Waiting for handler to shut down')
            await self._handler.shutdown(self.handler_shutdown_timeout)

    async def _cleanup_app(self) -> None:
        if self.web_app is not None:
            self.log.info('Cleanup')
            await self.web_app.cleanup()

    @property
    def _app(self) -> Application:
        # XXX compat alias
        return self.web_app
