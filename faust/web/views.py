from typing import Any, Awaitable, Callable, Mapping, Type, cast
from .base import Request, Web
from ..types import AppT

__all__ = ['View', 'Site']


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
            'put': self.put,
        }

    async def dispatch(self, request: Any):
        return await self.methods[request.method.lower()](
            self.web,
            cast(Request, request))

    async def get(self, web: Web, request: Request) -> Any:
        ...

    async def post(self, web: Web, request: Request) -> Any:
        ...

    async def put(self, web: Web, request: Request) -> Any:
        ...

    async def patch(self, web: Web, request: Request) -> Any:
        ...

    async def delete(self, web: Web, request: Request) -> Any:
        ...


class Site:
    views: Mapping[str, Type]

    def __init__(self, app: AppT) -> None:
        self.app = app

    def enable(self, web: Web, *, prefix: str = '') -> None:
        for pattern, view in self.views.items():
            web.route(prefix + pattern, view(self.app, web).dispatch)
