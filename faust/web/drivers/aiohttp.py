"""Web driver using :pypi:`aiohttp`."""
import asyncio
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable, ClassVar, Optional, Tuple, Union, cast

from aiohttp import __version__ as aiohttp_version
from aiohttp.web import (
    AppRunner,
    Application,
    Response,
    TCPSite,
    UnixSite,
    json_response,
)
from mode import Service
from mode.threads import ServiceThread
from mode.utils.compat import want_str
from mode.utils.futures import notify
from mode.utils.objects import cached_property

from faust.types import AppT
from faust.utils import json as _json
from faust.web import base

__all__ = ['Web']

CONTENT_SEPARATOR: bytes = b'\r\n\r\n'
HEADER_SEPARATOR: bytes = b'\r\n'
HEADER_KEY_VALUE_SEPARATOR: bytes = b': '

_bytes = bytes


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
        await self.web.start_server()
        notify(self._port_open)  # <- .start() can return now

    async def crash(self, exc: BaseException) -> None:
        if self._port_open and not self._port_open.done():
            self._port_open.set_exception(exc)  # <- .start() will raise
        await super().crash(exc)

    async def on_thread_stop(self) -> None:
        # on_stop() executes in parent thread, on_thread_stop in the thread.
        await self.web.stop_server()


class WebService(Service):

    #: We serve the web server in a separate thread (and separate event loop).
    _thread: Optional[ServerThread] = None

    def __init__(self, web: 'Web', **kwargs: Any) -> None:
        self.web = web
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        self.web.init_server()
        self._thread = ServerThread(
            self.web, loop=self.loop, beacon=self.beacon)
        self.add_dependency(self._thread)


class Web(base.Web):
    """Web server and framework implemention using :pypi:`aiohttp`."""

    driver_version = f'aiohttp={aiohttp_version}'
    handler_shutdown_timeout: float = 60.0

    content_separator: ClassVar[bytes] = CONTENT_SEPARATOR
    header_separator: ClassVar[bytes] = HEADER_SEPARATOR
    header_key_value_separator: ClassVar[bytes] = HEADER_KEY_VALUE_SEPARATOR

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(app, **kwargs)
        self.web_app: Application = Application()
        self._runner: AppRunner = AppRunner(self.web_app)

    @cached_property
    def _service(self) -> WebService:
        return WebService(self, loop=self.app.loop, beacon=self.app.beacon)

    async def wsgi(self) -> Any:
        self.init_server()
        return self.web_app

    def text(self, value: str, *, content_type: str = None,
             status: int = 200) -> base.Response:
        response = Response(
            text=value, content_type=content_type, status=status)
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
            body=value, status=status, content_type=content_type)
        return cast(base.Response, response)

    def route(self, pattern: str, handler: Callable) -> None:
        self.web_app.router.add_route('*', pattern, handler)

    def add_static(self, prefix: str, path: Union[Path, str],
                   **kwargs: Any) -> None:
        self.web_app.router.add_static(prefix, str(path), **kwargs)

    def bytes_to_response(self, s: _bytes) -> base.Response:
        status_code, _, payload = s.partition(self.content_separator)
        headers, _, body = payload.partition(self.content_separator)

        response = Response(
            body=body,
            status=HTTPStatus(int(status_code)),
            headers=dict(self._splitheader(h) for h in headers.splitlines()),
        )
        return cast(base.Response, response)

    def _splitheader(self, header: _bytes) -> Tuple[str, str]:
        key, value = header.split(self.header_key_value_separator, 1)
        return want_str(key.strip()), want_str(value.strip())

    def response_to_bytes(self, response: base.Response) -> _bytes:
        resp = cast(Response, response)
        return self.content_separator.join([
            str(resp.status).encode(),
            self.content_separator.join(
                [self._headers_serialize(resp), resp.body]),
        ])

    def _headers_serialize(self, response: Response) -> _bytes:
        return self.header_separator.join(
            self.header_key_value_separator.join([
                k if isinstance(k, _bytes) else k.encode('ascii'),
                v if isinstance(v, _bytes) else v.encode('latin-1'),
            ]) for k, v in response.headers.items())

    def _create_site(self) -> Optional[Union[TCPSite, UnixSite]]:
        site = None
        transport = self.app.conf.web_transport.scheme

        if transport == 'tcp':
            site = TCPSite(self._runner, self.app.conf.web_bind,
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
