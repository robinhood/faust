"""Class-based views."""
from typing import Any, Awaitable, Callable, Mapping, Type, cast
from faust.types import AppT
from .base import Request, Response, Web

__all__ = ['View', 'Site']

_bytes = bytes   # need alias for method named `bytes`


class View:
    """View (HTTP endpoint)."""

    package: str = None
    methods: Mapping[str, Callable[[Request], Awaitable]]

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

    async def dispatch(self, request: Any) -> None:
        return await self.methods[request.method.lower()](
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

    def text(self, value: str,
             *,
             content_type: str = None,
             status: int = 200) -> Response:
        return self.web.text(value, content_type=content_type, status=status)

    def html(self, value: str,
             *,
             status: int = 200) -> Response:
        return self.web.html(value, status=status)

    def json(self, value: Any,
             *,
             status: int = 200) -> Response:
        return self.web.json(value, status=status)

    def bytes(self, value: _bytes,
              *,
              content_type: str = None,
              status: int = 200) -> Response:
        return self.web.bytes(value, content_type=content_type, status=status)

    def route(self, pattern: str, handler: Callable) -> None:
        return self.web.route(pattern, handler)

    def notfound(self, reason: str = 'Not Found', **kwargs: Any) -> Response:
        return self.error(404, reason, **kwargs)

    def error(self, status: int, reason: str, **kwargs: Any) -> Response:
        return self.json({'error': reason, **kwargs}, status=status)


class Site:
    """Collection of HTTP endpoints (views)."""

    views: Mapping[str, Type[View]]

    def __init__(self, app: AppT) -> None:
        self.app = app

    def enable(self, web: Web, *, prefix: str = '') -> None:
        for pattern, view in self.views.items():
            web.route(prefix + pattern, view(self.app, web).dispatch)
