from typing import Any, Callable, cast
from aiohttp import __version__ as aiohttp_version
from aiohttp.web import Application, Response, json_response
from faust.utils.logging import get_logger
from .. import base

__all__ = ['Web']

DEFAULT_PORT = 8080
DEFAULT_BIND = '0.0.0.0'

_bytes = bytes

logger = get_logger(__name__)


class Web(base.Web):
    logger = logger

    driver_version = f'aiohttp={aiohttp_version}'

    def __init__(self, *,
                 port: int = None,
                 bind: str = None,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app: Application = Application()
        self.port: int = port or DEFAULT_PORT
        self.bind: str = bind or DEFAULT_BIND
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
        self.app.router.add_route('*', pattern, handler)

    async def on_start(self) -> None:
        self._handler = self.app.make_handler()
        self._srv = await self.loop.create_server(
            self._handler, self.bind, self.port)
        self.log.info('Serving on %s', self.url)

    async def on_stop(self) -> None:
        self.log.info('Closing server')
        self._srv.close()
        self.log.info('Waiting for server to close handle')
        await self._srv.wait_closed()
        self.log.info('Shutting down web application')
        await self.app.shutdown()
        self.log.info('Waiting for handler to shut down')
        await self._handler.shutdown(60.0)
        self.log.info('Cleanup')
        await self.app.cleanup()
