"""Website served by the Faust web server."""
from typing import Any, Sequence, Tuple, Type, Union

from mode import Service

from faust.types import AppT
from faust.types.web import BlueprintT, View

from . import drivers
from .apps import graph
from .apps import router
from .apps import stats
from .apps import tables
from .base import Web

__all__ = ['Website']

DEFAULT_DRIVER = 'aiohttp://'


class Website(Service):
    """Service starting the Faust web server and endpoints."""

    web: Web
    app: AppT
    port: int
    bind: str

    blueprints: Sequence[Tuple[str, BlueprintT]] = [
        ('/graph', graph.blueprint),
        ('', stats.blueprint),
        ('/router', router.blueprint),
        ('/table', tables.blueprint),
    ]

    def __init__(self,
                 app: AppT,
                 *,
                 port: int = None,
                 bind: str = None,
                 driver: Union[Type[Web], str] = DEFAULT_DRIVER,
                 extra_pages: Sequence[Tuple[str, Type[View]]] = None,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.port = port or 6066
        self.bind = bind or 'localhost'
        self.init_driver(driver, **kwargs)
        self.init_pages(extra_pages or [])
        self.init_webserver()
        self.add_dependency(self.web)

    def init_driver(self, driver: Union[Type[Web], str],
                    **kwargs: Any) -> None:
        web_cls: Type[Web] = drivers.by_url(driver)
        self.web: Web = web_cls(
            self.app,
            port=self.port,
            bind=self.bind,
            **kwargs)

    def init_pages(self,
                   extra_pages: Sequence[Tuple[str, Type[View]]]) -> None:
        app = self.app
        for prefix, blueprint in self.blueprints:
            blueprint.register(app, url_prefix=prefix)
        for prefix, page in list(app.pages) + list(extra_pages or []):
            self.web.add_view(page, prefix=prefix)

    def init_webserver(self) -> None:
        self.app.on_webserver_init(self.web)
        for _, blueprint in self.blueprints:
            blueprint.init_webserver(self.web)
        for blueprint in self.app._blueprints.values():
            blueprint.init_webserver(self.web)
