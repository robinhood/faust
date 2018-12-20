"""Web driver using :pypi:`aiohttp`."""
from pathlib import Path
from typing import Any, Callable, Optional, Union, cast

from aiohttp import __version__ as aiohttp_version
from aiohttp.web import (
    AppRunner,
    Application,
    Request,
    Response,
    TCPSite,
    UnixSite,
    json_response,
)
from faust.types import AppT
from faust.utils import json as _json
from faust.web import base
from mode import Service
from mode.threads import ServiceThread

__all__ = ['Web']

_bytes = bytes


class ServerThread(ServiceThread):

    def __init__(self, web: 'Web', **kwargs: Any) -> None:
        self.web = web
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        await self.web.start_server()

    async def on_thread_stop(self) -> None:
        # on_stop() executes in parent thread, on_thread_stop in the thread.
        await self.web.stop_server()


class Server(Service):

    def __init__(self, web: 'Web', **kwargs: Any) -> None:
        self.web = web
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        await self.web.start_server()

    async def on_stop(self) -> None:
        await self.web.stop_server()


class Web(base.Web):
    """Web server and framework implemention using :pypi:`aiohttp`."""

    driver_version = f'aiohttp={aiohttp_version}'
    handler_shutdown_timeout: float = 60.0

    #: We serve the web server in a separate thread (and separate event loop).
    _thread: Optional[Service] = None

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(app, **kwargs)
        self.web_app: Application = Application()
        self._runner: AppRunner = AppRunner(self.web_app, access_log=None)

    async def on_start(self) -> None:
        self.init_server()
        server_cls = ServerThread if self.app.conf.web_in_thread else Server
        self._thread = server_cls(self, loop=self.loop, beacon=self.beacon)
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

    def _create_site(self) -> Optional[Union[TCPSite, UnixSite]]:
        site = None
        transport = self.app.conf.web_transport.scheme

        if transport == 'tcp':
            site = TCPSite(
                self._runner,
                self.app.conf.web_bind,
                self.app.conf.web_port)
        elif transport == 'unix':
            site = UnixSite(self._runner, self.app.conf.web_transport.path)

        return site

    async def start_server(self) -> None:
        await self._runner.setup()
        site = self._create_site()

        if site is not None:
            await site.start()

    async def stop_server(self) -> None:
        if self._runner:
            await self._runner.cleanup()
        await self._cleanup_app()

    async def _cleanup_app(self) -> None:
        if self.web_app is not None:
            self.log.info('Cleanup')
            await self.web_app.cleanup()

    @property
    def _app(self) -> Application:
        # XXX compat alias
        return self.web_app
