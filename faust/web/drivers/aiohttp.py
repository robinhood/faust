import aiohttp
from typing import Any, Callable, cast
from aiohttp.web import Application, Response, json_response
from faust.utils.logging import get_logger
from .. import base

__all__ = ['Web']

DEFAULT_PORT = 8080
DEFAULT_BIND = '0.0.0.0'

_bytes = bytes

logger = get_logger(__name__)


class Web(base.Web):

    driver_version = 'aiohttp={}'.format(aiohttp.__version__)

    def __init__(self, *,
                 port: int = DEFAULT_PORT,
                 bind: str = DEFAULT_BIND,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app: Application = Application()
        self.port: int = port
        self.bind: str = bind
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
        logger.info('Web: Serving on %s', self.url)

    async def on_stop(self) -> None:
        logger.info('Web: closing server')
        self._srv.close()
        logger.info('Web: waiting for server to close handle')
        await self._srv.wait_closed()
        logger.info('Web: Shutting down web application')
        await self.app.shutdown()
        logger.info('Web: Waiting for handler to shut down')
        await self._handler.shutdown(60.0)
        logger.info('Web: cleanup')
        await self.app.cleanup()
