from pathlib import Path
from typing import List, NamedTuple, Optional, Type, Union

from faust.types import AppT
from faust.types.web import BlueprintT, PageArg, RouteDecoratorRet, View

__all__ = ['Blueprint']


class FutureRoute(NamedTuple):
    uri: str
    name: Optional[str]
    handler: PageArg
    base: Type[View]


class FutureStaticRoute(NamedTuple):
    uri: str
    file_or_directory: Path
    name: str


class Blueprint(BlueprintT):
    routes: List[FutureRoute]
    static_routes: List[FutureStaticRoute]

    def __init__(self,
                 name: str,
                 *,
                 url_prefix: Optional[str] = None) -> None:
        self.name = name
        self.url_prefix = url_prefix

        self.routes = []
        self.static_routes = []

    def route(self,
              uri: str,
              *,
              name: Optional[str] = None,
              base: Type[View] = View) -> RouteDecoratorRet:
        def _inner(handler: PageArg) -> PageArg:
            route = FutureRoute(uri, name, handler, base)
            self.routes.append(route)
            return handler
        return _inner

    def static(self,
               uri: str,
               file_or_directory: Union[str, Path],
               *,
               name: Optional[str] = None) -> None:
        _name: str = name or 'static'
        if not _name.startswith(self.name + '.'):
            _name = f'{self.name}.{name}'
        fut = FutureStaticRoute(uri, Path(file_or_directory), _name)
        self.static_routes.append(fut)

    def register(self, app: AppT,
                 *,
                 url_prefix: Optional[str] = None) -> None:
        url_prefix = url_prefix or self.url_prefix

        # Routes
        for route in self.routes:
            self._apply_route(app, route, url_prefix)

        # Static Routes
        for static_route in self.static_routes:
            self._apply_static_route(app, static_route, url_prefix)

    def _apply_route(self,
                     app: AppT,
                     route: FutureRoute,
                     url_prefix: Optional[str]) -> None:
        uri = url_prefix + route.uri if url_prefix else route.uri

        app.page(path=uri[1:] if uri.startswith('//') else uri,
                 name=route.name)(route.handler)

    def _apply_static_route(self,
                            app: AppT,
                            route: FutureStaticRoute,
                            url_prefix: Optional[str]) -> None:
        uri = url_prefix + route.uri if url_prefix else route.uri
        app.static(uri, route.file_or_directory, name=route.name)
