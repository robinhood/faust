import abc
import typing
from pathlib import Path
from typing import Awaitable, Callable, Optional, Type, Union

from aiohttp.client import ClientSession

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
    'BlueprintT',
]

ViewGetHandler = Callable[..., Awaitable[Response]]
RoutedViewGetHandler = Callable[[ViewGetHandler], ViewGetHandler]
PageArg = Union[Type[View], ViewGetHandler]
RouteDecoratorRet = Callable[[PageArg], PageArg]


class HttpClientT(ClientSession):
    ...


class BlueprintT(abc.ABC):
    name: str
    url_prefix: Optional[str]

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
