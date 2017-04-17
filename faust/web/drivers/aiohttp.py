from typing import Any, Callable
from aiohttp.web import Application, Response
from .. import base


class Web(base.Web):

    def on_init(self) -> None:
        self.app = Application()
        self.srv = None
        self.handler = None

    def text(self, value: str) -> Any:
        return Response(text=value)

    def bytes(self, value: bytes, content_type: str) -> Any:
        return Response(body=value, content_type=content_type)

    def route(self, pattern: str, handler: Callable) -> None:
        self.app.router.add_route('*', pattern, handler)

    async def on_start(self) -> None:
        self.handler = self.app.make_handler()
        self.srv = await self.loop.create_server(self.handler, '0.0.0.0', 8080)
        print('serving on {}'.format(self.srv.sockets[0].getsockname()))

    async def on_stop(self) -> None:
        await self.srv.wait_closed()
        await self.app.shutdown()
        await self.handler.shutdown(60.0)
        await self.app.cleanup()
