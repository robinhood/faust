"""Class-based views."""
from typing import Any, Awaitable, Callable, Mapping, Type, cast, no_type_check

from faust.types import AppT
from faust.types.web import ViewGetHandler

from .base import Request, Response, Web

__all__ = ['View']

_bytes = bytes   # need alias for method named `bytes`


class View:
    """View (HTTP endpoint)."""
    view_name: str
    view_path: str

    methods: Mapping[str, Callable[[Request], Awaitable]]

    @classmethod
    def from_handler(cls, fun: ViewGetHandler) -> Type['View']:
        if not callable(fun):
            raise TypeError(f'View handler must be callable, not {fun!r}')
        return type(fun.__name__, (cls,), {
            'get': fun,
            '__doc__': fun.__doc__,
            '__module__': fun.__module__,
        })

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

    async def __call__(self, request: Any) -> Any:
        return await self.dispatch(request)

    async def dispatch(self, request: Any) -> Any:
        method = request.method.lower()
        kwargs = request.match_info or {}  # XXX Aiohttp specific

        # we cast here since some subclasses take extra parameters
        # from the URL route (match_info).
        method = cast(Callable[..., Awaitable[Response]], self.methods[method])
        return await method(cast(Request, request), **kwargs)

    @no_type_check  # subclasses change signature based on route match_info
    async def get(self, request: Request) -> Any:
        ...

    @no_type_check  # subclasses change signature based on route match_info
    async def post(self, request: Request) -> Any:
        ...

    @no_type_check  # subclasses change signature based on route match_info
    async def put(self, request: Request) -> Any:
        ...

    @no_type_check  # subclasses change signature based on route match_info
    async def patch(self, request: Request) -> Any:
        ...

    @no_type_check  # subclasses change signature based on route match_info
    async def delete(self, request: Request) -> Any:
        ...

    def text(self, value: str, *, content_type: str = None,
             status: int = 200) -> Response:
        return self.web.text(value, content_type=content_type, status=status)

    def html(self, value: str, *, status: int = 200) -> Response:
        return self.web.html(value, status=status)

    def json(self, value: Any, *, status: int = 200) -> Response:
        return self.web.json(value, status=status)

    def bytes(self,
              value: _bytes,
              *,
              content_type: str = None,
              status: int = 200) -> Response:
        return self.web.bytes(value, content_type=content_type, status=status)

    def route(self, pattern: str, handler: Callable) -> Any:
        self.web.route(pattern, handler)
        return handler

    def notfound(self, reason: str = 'Not Found', **kwargs: Any) -> Response:
        return self.error(404, reason, **kwargs)

    def error(self, status: int, reason: str, **kwargs: Any) -> Response:
        return self.json({'error': reason, **kwargs}, status=status)
