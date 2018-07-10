"""Web driver using :pypi:`aiohttp`."""
import asyncio
from pathlib import Path
from typing import Any, Callable, Optional, Union, cast

from aiohttp import __version__ as aiohttp_version
from aiohttp.web import Application, Response, json_response
from mode.threads import ServiceThread
from mode.utils.futures import notify

from faust.types import AppT
from faust.web import base

__all__ = ['Web']

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

    driver_version = f'aiohttp={aiohttp_version}'
    handler_shutdown_timeout: float = 60.0

    #: We serve the web server in a separate thread (and separate event loop).
    _thread: Optional[ServerThread] = None

    def __init__(self,
                 app: AppT,
                 *,
                 port: int = None,
                 bind: str = None,
                 **kwargs: Any) -> None:
        super().__init__(app, port=port, bind=bind, **kwargs)
        self._app: Application = Application()
        self._srv: Any = None
        self._handler: Any = None

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
        return json_response(value, status=status)

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

    def route(self, pattern: str, handler: Callable) -> None:
        self._app.router.add_route('*', pattern, handler)

    def add_static(self,
                   prefix: str,
                   path: Union[Path, str],
                   **kwargs: Any) -> None:
        self._app.router.add_static(prefix, str(path), **kwargs)

    async def on_start(self) -> None:
        self._thread = ServerThread(self, loop=self.loop, beacon=self.beacon)
        self.add_dependency(self._thread)

    async def start_server(self, loop: asyncio.AbstractEventLoop) -> None:
        handler = self._handler = self._app.make_handler()
        self._srv = await loop.create_server(handler, self.bind, self.port)
        self.log.info('Serving on %s', self.url)

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
        if self._app is not None:
            self.log.info('Shutting down web application')
            await self._app.shutdown()

    async def _shutdown_handler(self) -> None:
        if self._handler is not None:
            self.log.info('Waiting for handler to shut down')
            await self._handler.shutdown(self.handler_shutdown_timeout)

    async def _cleanup_app(self) -> None:
        if self._app is not None:
            self.log.info('Cleanup')
            await self._app.cleanup()
