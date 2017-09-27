import asyncio
from typing import Any, Callable, cast
from aiohttp import __version__ as aiohttp_version
from aiohttp.web import Application, Response, json_response
from mode import Service
from mode.threads import ServiceThread
from .. import base
from ...types import AppT

__all__ = ['Web']

_bytes = bytes


class Web(base.Web):

    driver_version = f'aiohttp={aiohttp_version}'

    #: We serve the web server in a separate thread, with its own even loop.
    _thread: ServiceThread = None

    class WebserverServiceThread(Service):

        def __init__(self, web: 'Web', **kwargs: Any) -> None:
            self.web = web
            super().__init__(**kwargs)

        async def on_start(self) -> None:
            await self.web.start_server(self.loop)

        async def on_stop(self) -> None:
            await self.web.stop_server(self.loop)

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
        self._thread = ServiceThread(self.WebserverServiceThread(
            self,
            beacon=self.beacon,
        ))
        self._thread.start()

    async def on_stop(self) -> None:
        self._thread.stop()

    async def start_server(self, loop: asyncio.AbstractEventLoop) -> None:
        self._handler = self._app.make_handler()
        self._srv = await loop.create_server(
            self._handler, self.bind, self.port)
        self.log.info('Serving on %s', self.url)

    async def stop_server(self, loop: asyncio.AbstractEventLoop) -> None:
        self.log.info('Closing server')
        self._srv.close()
        self.log.info('Waiting for server to close handle')
        await self._srv.wait_closed()
        self.log.info('Shutting down web application')
        await self._app.shutdown()
        self.log.info('Waiting for handler to shut down')
        await self._handler.shutdown(60.0)
        self.log.info('Cleanup')
        await self._app.cleanup()
