"""Class-based views."""
import inspect
from typing import (
    Any,
    Awaitable,
    Callable,
    List,
    Mapping,
    Optional,
    Type,
    cast,
)

from faust.types import AppT
from faust.types.web import PageArg, ViewGetHandler

from .base import Request, Response, Web

__all__ = ['View', 'Site']

CommandDecorator = Callable[[PageArg], Type['Site']]

_bytes = bytes   # need alias for method named `bytes`


class View:
    """View (HTTP endpoint)."""

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
        return await self.methods[method](cast(Request, request))

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


class Site:
    """Collection of HTTP endpoints (views)."""

    views: Mapping[str, Type[View]]

    def __init__(self, app: AppT) -> None:
        self.app = app

    def enable(self, web: Web, *, prefix: str = '') -> List[View]:
        return [
            self._route(web, view_cls, prefix + pattern)
            for pattern, view_cls in self.views.items()
        ]

    def _route(self, web: Web, view_cls: Type[View], pattern: str) -> View:
        view = view_cls(self.app, web)
        web.route(pattern, view)
        return view

    @classmethod
    def from_handler(cls, path: str, *,
                     base: Type[View] = None) -> CommandDecorator:
        view_base: Type[View] = base if base is not None else View

        def _decorator(fun: PageArg) -> Type[Site]:
            view: Optional[Type[View]] = None
            if inspect.isclass(fun):
                view = cast(Type[View], fun)
                if not issubclass(view, View):
                    raise TypeError(
                        'When decorating class, it must be subclass of View')
            if view is None:
                view = view_base.from_handler(cast(ViewGetHandler, fun))

            return type('Site', (cls,), {
                'views': {
                    path: view,
                },
                '__module__': fun.__module__,
            })

        return _decorator
