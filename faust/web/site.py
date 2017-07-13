from typing import Any, Sequence, Tuple, Type, Union
from .apps import graph
from .apps import stats
from .base import Web
from .views import Site
from ..types import AppT
from ..utils.imports import symbol_by_name
from ..utils.services import Service

__all__ = ['Website']

DEFAULT_DRIVER = 'faust.web.drivers.aiohttp:Web'


class Website(Service):
    web: Web

    pages: Sequence[Tuple[str, Type[Site]]] = [
        ('/graph', graph.Site),
        ('', stats.Site),
    ]

    def __init__(self, app: AppT,
                 *,
                 port: int = None,
                 bind: str = None,
                 driver: Union[Type[Web], str] = DEFAULT_DRIVER,
                 extra_pages: Sequence[Tuple[str, Type[Site]]] = None,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        web_cls: Type[Web] = symbol_by_name(driver)
        self.web: Web = web_cls(app, port=port, bind=bind, **kwargs)
        for prefix, page in list(self.pages) + list(extra_pages or []):
            page(app).enable(self.web, prefix=prefix)
        self.add_dependency(self.web)
