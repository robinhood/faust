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
import abc
import typing
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Type,
    Union,
    cast,
)

from mode.utils.objects import iter_mro_reversed
from mode.utils.times import Seconds

from faust.types import AppT
from faust.types.web import (
    BlueprintT,
    CacheBackendT,
    CacheT,
    PageArg,
    RouteDecoratorRet,
    View,
    Web,
)

if typing.TYPE_CHECKING:
    from faust.app import App
else:
    class App: ...  # noqa

__all__ = ['Blueprint']


class ResolveMe:
    # signifies a promise that needs to be unwrapped

    @abc.abstractmethod
    def resolve(self, app: AppT, view: Type[View]) -> Any:
        ...


class FutureCachedView(ResolveMe):
    # Unresolved @cache.view decorator

    def __init__(self,
                 fun: Callable,
                 cache: 'FutureCache',
                 timeout: Optional[Seconds],
                 key_prefix: Optional[str],
                 options: Dict[str, Any]) -> None:
        self.fun: Callable = fun
        self.cache: 'FutureCache' = cache
        self.timeout: Optional[Seconds] = timeout
        self.key_prefix: Optional[str] = key_prefix
        self.options: Dict[str, Any] = options

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fun(*args, **kwargs)

    def resolve(self, app: AppT, view: Type[View]) -> Any:
        # Calls real cache.view(decorated_fun)
        # and return value is stored as the actual atttribute on
        # the resolved view.
        return self.cache.resolve(app).view(
            timeout=self.timeout,
            key_prefix=self.key_prefix,
            **self.options)(self.fun)


class FutureCache(CacheT):

    blueprint: BlueprintT

    # Every app must have separate faust.web.Cache instance.
    # It's fine to cache this as apps are normally global
    # and exists until the process dies.
    _cache_for_app: Dict[AppT, CacheT]

    def __init__(self,
                 timeout: Seconds = None,
                 key_prefix: str = '',
                 backend: Union[Type[CacheBackendT], str] = None,
                 *,
                 blueprint: BlueprintT,
                 **kwargs: Any) -> None:
        self.blueprint = blueprint
        self.timeout = timeout
        if key_prefix is None:
            key_prefix = blueprint.name
        self.key_prefix = key_prefix
        self.backend = backend
        self._cache_for_app = {}

    def view(self,
             timeout: Seconds = None,
             key_prefix: str = None,
             **kwargs: Any) -> Callable[[Callable], Callable]:
        def _inner(fun: Callable) -> Callable:
            return FutureCachedView(fun, self, timeout, key_prefix, kwargs)
        return _inner

    def resolve(self, app: AppT) -> CacheT:
        try:
            return self._cache_for_app[app]
        except KeyError:
            cache = self._cache_for_app[app] = self._resolve(app)
            return cache

    def _resolve(self, app: AppT) -> CacheT:
        from faust.web.cache import Cache
        return Cache(
            timeout=self.timeout,
            key_prefix=self.key_prefix,
            backend=self.backend,
            app=app,
        )


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

    def cache(self,
              timeout: Seconds = None,
              key_prefix: str = '',
              backend: Union[Type[CacheBackendT], str] = None) -> CacheT:
        return FutureCache(timeout, key_prefix, backend, blueprint=self)

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

        # Create the actual view on the app (using app.page)

        view: Type[View] = app.page(
            path=uri[1:] if uri.startswith('//') else uri,
            name=self._view_name(route.name),
        )(route.handler)

        # Resolve any ResolveMe attributes set on the view.
        self._resolve_page_futures(app, view)

    def _resolve_page_futures(self, app: AppT, view: Type[View]) -> None:
        from faust import web  # View in this module is just a type
        for subcls in iter_mro_reversed(view, stop=web.View):
            resolved = {}
            for key, value in vars(subcls).items():
                if isinstance(value, ResolveMe):
                    resolved[key] = value.resolve(app, view)
            for key, value in resolved.items():
                setattr(subcls, key, value)

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
