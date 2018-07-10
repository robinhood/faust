"""Website served by the Faust web server."""
from typing import Any, Sequence, Tuple, Type, Union

from mode import Service

from faust.types import AppT

from . import drivers
from .apps import graph
from .apps import router
from .apps import stats
from .apps import tables
from .base import Web
from .views import Site

__all__ = ['Website']

DEFAULT_DRIVER = 'aiohttp://'


class Website(Service):
    """Service starting the Faust web server and endpoints."""

    web: Web
    app: AppT
    port: int
    bind: str

    pages: Sequence[Tuple[str, Type[Site]]] = [
        ('/graph', graph.Site),
        ('', stats.Site),
        ('/router', router.Site),
        ('/table', tables.Site),
    ]

    def __init__(self,
                 app: AppT,
                 *,
                 port: int = None,
                 bind: str = None,
                 driver: Union[Type[Web], str] = DEFAULT_DRIVER,
                 extra_pages: Sequence[Tuple[str, Type[Site]]] = None,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.port = port or 6066
        self.bind = bind or 'localhost'
        self.init_driver(driver, **kwargs)
        self.init_pages(extra_pages or [])
        self.add_dependency(self.web)

    def init_driver(self, driver: Union[Type[Web], str],
                    **kwargs: Any) -> None:
        web_cls: Type[Web] = drivers.by_url(driver)
        self.web: Web = web_cls(
            self.app,
            port=self.port,
            bind=self.bind,
            **kwargs)
        self.app.on_webserver_init(self.web)

    def init_pages(self,
                   extra_pages: Sequence[Tuple[str, Type[Site]]]) -> None:
        app = self.app
        pages = list(self.pages) + list(app.pages) + list(extra_pages or [])
        for prefix, page in pages:
            page(app).enable(self.web, prefix=prefix)
