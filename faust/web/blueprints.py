from pathlib import Path
import typing
from typing import List, NamedTuple, Optional, Type, Union, cast

from faust.types import AppT
from faust.types.web import BlueprintT, PageArg, RouteDecoratorRet, View, Web

if typing.TYPE_CHECKING:
    from faust.app import App
else:
    class App: ...  # noqa

__all__ = ['Blueprint']


class FutureRoute(NamedTuple):
    uri: str
    name: str
    handler: PageArg
    base: Type[View]


class FutureStaticRoute(NamedTuple):
    uri: str
    file_or_directory: Path
    name: str


class Blueprint(BlueprintT):
    routes: List[FutureRoute]
    static_routes: List[FutureStaticRoute]
    view_name_separator: str = ':'

    def __init__(self,
                 name: str,
                 *,
                 url_prefix: Optional[str] = None) -> None:
        self.name = name
        self.url_prefix = url_prefix

        self.routes = []
        self.static_routes = []

    def clone(self, url_prefix: Optional[str] = None) -> BlueprintT:
        # Clone is only used for keeping track of static paths.
        # We have app.page(), but no app.static() so solved this
        # by keeping a mapping of the blueprint added to each app
        # (in app._blueprints).
        #
        # blueprint.register(app, uri_prefix) adds itself to this mapping
        # by creating a copy of the blueprint with that url_prefix set.

        if url_prefix is None:
            url_prefix = self.url_prefix
        bp = type(self)(name=self.name, url_prefix=url_prefix)

        # Note: The clone will not copy the list of routes, so any changes
        # to these in the clone will be reflected in the original.
        # (that is @app._blueprint['name'].route() will be added to all apps
        #  using that blueprint, pretty sure nobody would do this so
        #  it's safe).
        bp.routes = self.routes
        bp.static_routes = self.static_routes
        return bp

    def route(self,
              uri: str,
              *,
              name: Optional[str] = None,
              base: Type[View] = View) -> RouteDecoratorRet:
        def _inner(handler: PageArg) -> PageArg:
            route = FutureRoute(uri, name or handler.__name__, handler, base)
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

        # Apply routes
        for route in self.routes:
            self._apply_route(app, route, url_prefix)

        # Keep reference to blueprint on app,
        # so that it will call Blueprint.init_webserver when time
        # comes to add any @static paths.
        cast(App, app)._blueprints[self.name] = self.clone(
            url_prefix=url_prefix,
        )

    def _apply_route(self,
                     app: AppT,
                     route: FutureRoute,
                     url_prefix: Optional[str]) -> None:
        if url_prefix:
            uri = url_prefix.rstrip('/') + '/' + route.uri.lstrip('/')
        else:
            uri = route.uri

        app.page(
            path=uri[1:] if uri.startswith('//') else uri,
            name=self._view_name(route.name),
        )(route.handler)

    def _view_name(self, name: str) -> str:
        return self.view_name_separator.join([self.name, name])

    def init_webserver(self, web: Web) -> None:
        for route in self.static_routes:
            self._apply_static_route(web, route, self.url_prefix)
        self.on_webserver_init(web)

    def on_webserver_init(self, web: Web) -> None:
        ...

    def _apply_static_route(self,
                            web: Web,
                            route: FutureStaticRoute,
                            url_prefix: Optional[str]) -> None:
        uri = url_prefix + route.uri if url_prefix else route.uri
        web.add_static(uri, route.file_or_directory)
