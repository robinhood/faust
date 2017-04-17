from typing import Any, Awaitable, Callable, Mapping, cast
from .base import Request, Web
from ..types import AppT


class View:
    methods: Mapping[str, Callable[[Web, Request], Awaitable]]

    def __init__(self, app: AppT, web: Web) -> None:
        self.app = app
        self.web = web
        self.methods = {
            'get': self.get,
            'post': self.post,
            'patch': self.patch,
            'delete': self.delete,
            'put': self.put
        }

    async def dispatch(self, request: Any):
        return await self.methods[request.method.lower()](
            self.web,
            cast(Request, request))

    async def get(self, request: Request) -> Any:
        ...

    async def post(self, request: Request) -> Any:
        ...

    async def put(self, request: Request) -> Any:
        ...

    async def patch(self, request: Request) -> Any:
        ...

    async def delete(self, request: Request) -> Any:
        ...


class Site:

    def __init__(self, app):
        self.app = app

    def enable(self, web, *, prefix: str = ''):
        for pattern, view in self.views.items():
            web.route(prefix + pattern, view(self.app, web).dispatch)
