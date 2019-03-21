"""Class-based views."""
from functools import wraps
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Mapping,
    MutableMapping,
    Type,
    cast,
    no_type_check,
)

from faust.types import AppT, ModelT
from faust.types.web import ViewDecorator, ViewHandlerFun

from . import exceptions
from .base import Request, Response, Web
from .exceptions import WebError

__all__ = ['View', 'gives_model', 'takes_model']

_bytes = bytes   # need alias for method named `bytes`


class View:
    """Web view (HTTP endpoint)."""

    ServerError: ClassVar[Type[WebError]] = exceptions.ServerError
    ValidationError: ClassVar[Type[WebError]] = exceptions.ValidationError
    ParseError: ClassVar[Type[WebError]] = exceptions.ParseError
    NotAuthenticated: ClassVar[Type[WebError]] = exceptions.NotAuthenticated
    PermissionDenied: ClassVar[Type[WebError]] = exceptions.PermissionDenied
    NotFound: ClassVar[Type[WebError]] = exceptions.NotFound

    view_name: str
    view_path: str

    methods: Mapping[str, Callable[[Request], Awaitable]]

    @classmethod
    def from_handler(cls, fun: ViewHandlerFun) -> Type['View']:
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
            'head': self.head,
            'get': self.get,
            'post': self.post,
            'patch': self.patch,
            'delete': self.delete,
            'put': self.put,
            'options': self.options,
        }
        self.__post_init__()

    def __post_init__(self) -> None:
        ...

    async def __call__(self, request: Any) -> Any:
        return await self.dispatch(request)

    async def dispatch(self, request: Any) -> Any:
        method = request.method.lower()
        kwargs = request.match_info or {}  # XXX Aiohttp specific

        # we cast here since some subclasses take extra parameters
        # from the URL route (match_info).
        method = cast(Callable[..., Awaitable[Response]], self.methods[method])

        try:
            return await method(cast(Request, request), **kwargs)
        except WebError as exc:
            return await self.on_request_error(request, exc)

    async def on_request_error(self,
                               request: Request,
                               exc: WebError) -> Response:
        return self.error(exc.code, exc.detail, **exc.extra_context)

    @no_type_check
    async def head(self, request: Request, **kwargs: Any) -> Any:
        return await self.get(request, **kwargs)

    @no_type_check  # subclasses change signature based on route match_info
    async def get(self, request: Request, **kwargs: Any) -> Any:
        raise exceptions.MethodNotAllowed('Method GET not allowed.')

    @no_type_check  # subclasses change signature based on route match_info
    async def post(self, request: Request, **kwargs: Any) -> Any:
        raise exceptions.MethodNotAllowed('Method POST not allowed.')

    @no_type_check  # subclasses change signature based on route match_info
    async def put(self, request: Request, **kwargs: Any) -> Any:
        raise exceptions.MethodNotAllowed('Method PUT not allowed.')

    @no_type_check  # subclasses change signature based on route match_info
    async def patch(self, request: Request, **kwargs: Any) -> Any:
        raise exceptions.MethodNotAllowed('Method PATCH not allowed.')

    @no_type_check  # subclasses change signature based on route match_info
    async def delete(self, request: Request, **kwargs: Any) -> Any:
        raise exceptions.MethodNotAllowed('Method DELETE not allowed.')

    @no_type_check  # subclasses change signature based on route match_info
    async def options(self, request: Request, **kwargs: Any) -> Any:
        raise exceptions.MethodNotAllowed('Method OPTIONS not allowed.')

    def text(self, value: str, *,
             content_type: str = None,
             status: int = 200,
             reason: str = None,
             headers: MutableMapping = None) -> Response:
        return self.web.text(
            value,
            content_type=content_type,
            status=status,
            reason=reason,
            headers=headers,
        )

    def html(self, value: str, *,
             content_type: str = None,
             status: int = 200,
             reason: str = None,
             headers: MutableMapping = None) -> Response:
        return self.web.html(
            value,
            content_type=content_type,
            status=status,
            reason=reason,
            headers=headers,
        )

    def json(self, value: Any, *,
             content_type: str = None,
             status: int = 200,
             reason: str = None,
             headers: MutableMapping = None) -> Response:
        return self.web.json(
            value,
            content_type=content_type,
            status=status,
            reason=reason,
            headers=headers,
        )

    def bytes(self,
              value: _bytes,
              *,
              content_type: str = None,
              status: int = 200,
              reason: str = None,
              headers: MutableMapping = None) -> Response:
        return self.web.bytes(
            value,
            content_type=content_type,
            status=status,
            reason=reason,
            headers=headers,
        )

    async def read_request_content(self, request: Request) -> _bytes:
        return await self.web.read_request_content(request)

    def bytes_to_response(self, s: _bytes) -> Response:
        return self.web.bytes_to_response(s)

    def response_to_bytes(self, response: Response) -> _bytes:
        return self.web.response_to_bytes(response)

    def route(self, pattern: str, handler: Callable) -> Any:
        self.web.route(pattern, handler)
        return handler

    def notfound(self, reason: str = 'Not Found', **kwargs: Any) -> Response:
        # Deprecated: Use raise NotFound() instead.
        return self.error(404, reason, **kwargs)

    def error(self, status: int, reason: str, **kwargs: Any) -> Response:
        return self.json({'error': reason, **kwargs}, status=status)


def takes_model(Model: Type[ModelT]) -> ViewDecorator:
    """Decorate view function to return model data."""
    def _decorate_view(fun: ViewHandlerFun) -> ViewHandlerFun:
        @wraps(fun)
        async def _inner(view: View, request: Request,
                         *args: Any, **kwargs: Any) -> Response:
            data: bytes = await view.read_request_content(request)
            obj: ModelT = Model.loads(data, serializer='json')
            return await fun(  # type: ignore
                view, request, obj, *args, **kwargs)
        return _inner
    return _decorate_view


def gives_model(Model: Type[ModelT]) -> ViewDecorator:
    """Decorate view function to automatically decode POST data.

    The POST data is decoded using the model you specify.
    """
    def _decorate_view(fun: ViewHandlerFun) -> ViewHandlerFun:
        @wraps(fun)
        async def _inner(view: View, request: Request,
                         *args: Any, **kwargs: Any) -> Response:
            response: Any
            response = await fun(  # type: ignore
                view, request, *args, **kwargs)
            return view.json(response)
        return _inner
    return _decorate_view
