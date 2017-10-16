"""Website served by the Faust web server."""
from typing import Any, Sequence, Tuple, Type, Union
from mode import Service
from . import drivers
from .apps import graph
from .apps import router
from .apps import stats
from .base import Web
from .views import Site
from ..types import AppT

__all__ = ['Website']

DEFAULT_DRIVER = 'aiohttp://'


class Website(Service):
    """Service starting the Faust web server and endpoints."""

    web: Web

    pages: Sequence[Tuple[str, Type[Site]]] = [
        ('/graph', graph.Site),
        ('', stats.Site),
        ('/router', router.Site),
    ]

    def __init__(self, app: AppT,
                 *,
                 port: int = None,
                 bind: str = None,
                 driver: Union[Type[Web], str] = DEFAULT_DRIVER,
                 extra_pages: Sequence[Tuple[str, Type[Site]]] = None,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        web_cls: Type[Web] = drivers.by_url(driver)
        self.web: Web = web_cls(app, port=port, bind=bind, **kwargs)
        pages = list(self.pages) + list(app.pages) + list(extra_pages or [])
        for prefix, page in pages:
            page(app).enable(self.web, prefix=prefix)
        self.add_dependency(self.web)
