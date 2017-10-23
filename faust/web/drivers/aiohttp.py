"""Web driver using :pypi:`aiohttp`."""
import asyncio
from typing import Any, Callable, cast
from aiohttp import __version__ as aiohttp_version
from aiohttp.web import Application, Response, json_response
from mode.threads import ServiceThread
from mode.utils.futures import notify
from .. import base
from ...types import AppT

__all__ = ['Web']

_bytes = bytes


class ServerThread(ServiceThread):
    _port_open: asyncio.Future = None

    def __init__(self, web: 'Web', **kwargs: Any) -> None:
        self.web = web
        super().__init__(**kwargs)

    async def start(self) -> None:
        self._port_open = asyncio.Future(loop=self.parent_loop)
        await super().start()
        # thread exceptions do not propagate to the main thread, so we
        # need some way to communicate socket open errors, such as "port in
        # use", etc. back to the parent.  This future is set to an exception
        # if that happens, and it awaiting it here will reraise the error
        # in the parent thread.
        await self._port_open

    async def on_start(self) -> None:
        await self.web.start_server(self.loop)
        notify(self._port_open)  # <- .start() can return now

    async def crash(self, exc) -> None:
        if self._port_open and not self._port_open.done():
            self._port_open.set_exception(exc)  # <- .start() will raise
        await super().crash(exc)

    async def on_thread_stop(self) -> None:
        # on_stop() executes in parent thread, on_thread_stop in thread.
        await self.web.stop_server(self.loop)


class Web(base.Web):
    """Web server and framework implemention using :pypi:`aiohttp`."""

    driver_version = f'aiohttp={aiohttp_version}'

    #: We serve the web server in a separate thread, with its own even loop.
    _thread: ServerThread = None

    def __init__(self, app: AppT,
                 *,
                 port: int = None,
                 bind: str = None,
                 **kwargs: Any) -> None:
        super().__init__(app, port=port, bind=bind, **kwargs)
        self._app: Application = Application()
        self._srv: Any = None
        self._handler: Any = None

    def text(self, value: str, *, content_type: str = None) -> base.Response:
        return cast(base.Response, Response(
            text=value,
            content_type=content_type,
        ))

    def html(self, value: str) -> base.Response:
        return self.text(value, content_type='text/html')

    def json(self, value: Any) -> Any:
        return json_response(value)

    def bytes(self, value: _bytes, *, content_type: str = None) -> Any:
        return cast(base.Response, Response(
            body=value,
            content_type=content_type,
        ))

    def route(self, pattern: str, handler: Callable) -> None:
        self._app.router.add_route('*', pattern, handler)

    async def on_start(self) -> None:
        self._thread = ServerThread(self, loop=self.loop, beacon=self.beacon)
        self.add_dependency(self._thread)

    async def start_server(self, loop: asyncio.AbstractEventLoop) -> None:
        self._handler = self._app.make_handler()
        self._srv = await loop.create_server(
            self._handler, self.bind, self.port)
        self.log.info('Serving on %s', self.url)

    async def stop_server(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._srv is not None:
            self.log.info('Closing server')
            self._srv.close()
            self.log.info('Waiting for server to close handle')
            await self._srv.wait_closed()
        if self._app is not None:
            self.log.info('Shutting down web application')
            await self._app.shutdown()
        if self._handler is not None:
            self.log.info('Waiting for handler to shut down')
            await self._handler.shutdown(60.0)
        if self._app is not None:
            self.log.info('Cleanup')
            await self._app.cleanup()
