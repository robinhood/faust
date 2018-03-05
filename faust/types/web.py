import typing
from typing import Awaitable, Callable, Type, Union

from aiohttp.client import ClientSession

if typing.TYPE_CHECKING:
    from faust.web.base import Request, Response, Web
    from faust.web.views import Site, View
else:
    class Request: ...        # noqa
    class Response: ...       # noqa
    class Web: ...            # noqa
    class Site: ...           # noqa
    class View: ...           # noqa

__all__ = [
    'Request',
    'Response',
    'Site',
    'View',
    'ViewGetHandler',
    'RoutedViewGetHandler',
    'PageArg',
    'HttpClientT',
    'Web',
]

ViewGetHandler = Callable[[View, Request], Awaitable[Response]]
RoutedViewGetHandler = Callable[[ViewGetHandler], ViewGetHandler]
PageArg = Union[Type[View], ViewGetHandler]


class HttpClientT(ClientSession):
    ...
