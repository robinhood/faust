"""Blueprints define reusable web apps.

They are lazy and need to be registered to an app to be activated:

.. sourcecode:: python

    from faust import web

    blueprint = web.Blueprint('users')
    cache = blueprint.cache(timeout=300.0)

    @blueprint.route('/', name='list')
    class UserListView(web.View):

        @cache.view()
        async def get(self, request: web.Request) -> web.Response:
            return web.json(...)

    @blueprint.route('/{user_id}/', name='detail')
    class UserDetailView(web.View):

        @cache.view(timeout=10.0)
        async def get(self,
                      request: web.Request,
                      user_id: str) -> web.Response:
            return web.json(...)

At this point the views are realized and can be used
from Python code, but the cached ``get`` method handlers
cannot be called yet.

To actually use the view from a web server, we need to register
the blueprint to an app:

.. sourcecode:: python

    app = faust.App(
        'name',
        broker='kafka://',
        cache='redis://',
    )

    user_blueprint.register(app, url_prefix='/user/')

At this point the web server will have fully-realized views
with actually cached method handlers.

The blueprint is registered with a prefix, so the URL for the
``UserListView`` is now ``/user/``, and the URL for the ``UserDetailView``
is ``/user/{user_id}/``.

Blueprints can be registered to multiple apps at the same time.
"""
from pathlib import Path
from typing import List, Mapping, NamedTuple, Optional, Type, Union

from mode.utils.times import Seconds

from faust.types import AppT
from faust.types.web import (
    BlueprintT,
    CacheBackendT,
    CacheT,
    PageArg,
    ResourceOptions,
    RouteDecoratorRet,
    View,
    Web,
)

from .cache import Cache

__all__ = ['Blueprint']


class FutureRoute(NamedTuple):
    """Describes web route to be registered later."""

    uri: str
    name: str
    handler: PageArg
    base: Type[View]
    cors_options: Mapping[str, ResourceOptions]


class FutureStaticRoute(NamedTuple):
    """Describes static route to be registered later."""

    uri: str
    file_or_directory: Path
    name: str


class Blueprint(BlueprintT):
    """Define reusable web application."""

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

    def cache(self,
              timeout: Seconds = None,
              key_prefix: str = None,
              backend: Union[Type[CacheBackendT], str] = None) -> CacheT:
        if key_prefix is None:
            key_prefix = self.name
        return Cache(timeout, key_prefix, backend)

    def route(self,
              uri: str,
              *,
              name: Optional[str] = None,
              cors_options: Mapping[str, ResourceOptions] = None,
              base: Type[View] = View) -> RouteDecoratorRet:
        def _inner(handler: PageArg) -> PageArg:
            route = FutureRoute(
                uri=uri,
                name=name or handler.__name__,
                handler=handler,
                base=base,
                cors_options=cors_options or {},
            )
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

        for static_route in self.static_routes:
            self._apply_static_route(app.web, static_route, url_prefix)

    def _apply_route(self,
                     app: AppT,
                     route: FutureRoute,
                     url_prefix: Optional[str]) -> None:
        uri = self._url_with_prefix(route.uri, url_prefix)

        # Create the actual view on the app (using app.page)

        app.page(
            path=uri[1:] if uri.startswith('//') else uri,
            name=self._view_name(route.name),
            cors_options=route.cors_options,
        )(route.handler)

    def _view_name(self, name: str) -> str:
        return self.view_name_separator.join([self.name, name])

    def init_webserver(self, web: Web) -> None:
        self.on_webserver_init(web)

    def on_webserver_init(self, web: Web) -> None:
        ...

    def _url_with_prefix(self, url: str, prefix: str = None) -> str:
        if prefix:
            return prefix.rstrip('/') + '/' + url.lstrip('/')
        return url

    def _apply_static_route(self,
                            web: Web,
                            route: FutureStaticRoute,
                            url_prefix: Optional[str]) -> None:
        uri = self._url_with_prefix(route.uri, url_prefix)
        web.add_static(uri, route.file_or_directory)
