from typing import Any, Callable, cast
from aiohttp.web import Application, Response
from .. import base

DEFAULT_PORT = 8080
DEFAULT_BIND = '0.0.0.0'

_bytes = bytes


class Web(base.Web):

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

    def text(self, value: str) -> base.Response:
        return cast(base.Response, Response(text=value))

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
        sockets = self._srv.sockets  # type: ignore
        print('serving on {}'.format(sockets[0].getsockname()))

    async def on_stop(self) -> None:
        await self._srv.wait_closed()
        await self.app.shutdown()
        await self._handler.shutdown(60.0)
        await self.app.cleanup()
