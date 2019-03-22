import abc
import typing
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    NamedTuple,
    Optional,
    Sequence,
    Type,
    Union,
)

from aiohttp.client import ClientSession as HttpClientT
from mode import Seconds, ServiceT
from mypy_extensions import Arg, KwArg, VarArg
from yarl import URL

if typing.TYPE_CHECKING:
    from faust.types.app import AppT as _AppT
    from faust.web.base import Request, Response, Web
    from faust.web.views import View
else:
    class _AppT: ...          # noqa
    class Request: ...        # noqa
    class Response: ...       # noqa
    class Web: ...            # noqa
    class View: ...           # noqa

__all__ = [
    'Request',
    'Response',
    'ResourceOptions',
    'View',
    'ViewHandlerMethod',
    'ViewHandlerFun',
    'ViewDecorator',
    'PageArg',
    'HttpClientT',
    'Web',
    'CacheBackendT',
    'CacheT',
    'BlueprintT',
]

ViewHandlerMethod = Callable[
    [Arg(Request), VarArg(Any), KwArg(Any)],
    Awaitable[Response],
]

ViewHandler2ArgsFun = Callable[
    [Arg(View), Arg(Request)],
    Union[Coroutine[Any, Any, Response], Awaitable[Response]],
]

ViewHandlerVarArgsFun = Callable[
    [Arg(View), Arg(Request), VarArg(Any), KwArg(Any)],
    Union[Coroutine[Any, Any, Response], Awaitable[Response]],
]

ViewHandlerFun = Union[ViewHandler2ArgsFun, ViewHandlerVarArgsFun]

ViewGetHandler = ViewHandlerFun  # XXX compat
ViewDecorator = Callable[[ViewHandlerFun], ViewHandlerFun]
RoutedViewGetHandler = ViewDecorator  # XXX compat
PageArg = Union[Type[View], ViewHandlerFun]
RouteDecoratorRet = Callable[[PageArg], PageArg]


# Lists in ResourceOptions can be list of string or scalar '*'
# for matching any.
CORSListOption = Union[str, Sequence[str]]


class ResourceOptions(NamedTuple):
    """CORS Options for specific route, or defaults."""

    allow_credentials: bool = False
    expose_headers: Optional[CORSListOption] = ()
    allow_headers: Optional[CORSListOption] = ()
    max_age: Optional[int] = None
    allow_methods: Optional[CORSListOption] = ()


class CacheBackendT(ServiceT):

    @abc.abstractmethod
    def __init__(self,
                 app: _AppT,
                 url: Union[URL, str] = 'memory://',
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    async def get(self, key: str) -> Optional[bytes]:
        ...

    @abc.abstractmethod
    async def set(self, key: str, value: bytes, timeout: float) -> None:
        ...

    @abc.abstractmethod
    async def delete(self, key: str) -> None:
        ...


class CacheT(abc.ABC):

    timeout: Seconds
    key_prefix: str
    backend: Optional[Union[Type[CacheBackendT], str]]

    @abc.abstractmethod
    def __init__(self,
                 timeout: Seconds = None,
                 key_prefix: str = None,
                 backend: Union[Type[CacheBackendT], str] = None,
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def view(self,
             timeout: Seconds = None,
             key_prefix: str = None,
             **kwargs: Any) -> Callable[[Callable], Callable]:
        ...


class BlueprintT(abc.ABC):
    name: str
    url_prefix: Optional[str]

    @abc.abstractmethod
    def cache(self,
              timeout: Seconds = None,
              key_prefix: str = None,
              backend: Union[Type[CacheBackendT], str] = None) -> CacheT:
        ...

    @abc.abstractmethod
    def route(self,
              uri: str,
              *,
              name: Optional[str] = None,
              base: Type[View] = View) -> RouteDecoratorRet:
        ...

    @abc.abstractmethod
    def static(self,
               uri: str,
               file_or_directory: Union[str, Path],
               *,
               name: Optional[str] = None) -> None:
        ...

    @abc.abstractmethod
    def register(self, app: _AppT,
                 *,
                 url_prefix: Optional[str] = None) -> None:
        ...

    @abc.abstractmethod
    def init_webserver(self, web: Web) -> None:
        ...

    @abc.abstractmethod
    def on_webserver_init(self, web: Web) -> None:
        ...
