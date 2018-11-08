import abc
import typing
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Type, Union

from aiohttp.client import ClientSession as HttpClientT
from mode import Seconds, ServiceT
from yarl import URL

if typing.TYPE_CHECKING:
    from faust.types.app import AppT
    from faust.web.base import Request, Response, Web
    from faust.web.views import View
else:
    class AppT: ...           # noqa
    class Request: ...        # noqa
    class Response: ...       # noqa
    class Web: ...            # noqa
    class View: ...           # noqa

__all__ = [
    'Request',
    'Response',
    'View',
    'ViewGetHandler',
    'RoutedViewGetHandler',
    'PageArg',
    'HttpClientT',
    'Web',
    'CacheBackendT',
    'CacheT',
    'BlueprintT',
]

ViewGetHandler = Callable[..., Awaitable[Response]]
RoutedViewGetHandler = Callable[[ViewGetHandler], ViewGetHandler]
PageArg = Union[Type[View], ViewGetHandler]
RouteDecoratorRet = Callable[[PageArg], PageArg]


class CacheBackendT(ServiceT):

    @abc.abstractmethod
    def __init__(self,
                 app: AppT,
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
    def register(self, app: AppT,
                 *,
                 url_prefix: Optional[str] = None) -> None:
        ...

    @abc.abstractmethod
    def init_webserver(self, web: Web) -> None:
        ...

    @abc.abstractmethod
    def on_webserver_init(self, web: Web) -> None:
        ...
