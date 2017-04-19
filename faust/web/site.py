from typing import Any
from .drivers.aiohttp import Web
from ..types import AppT
from .apps import graph

__all__ = ['create_site']


def create_site(app: AppT, **kwargs: Any) -> Web:
    web = Web(**kwargs)
    g = graph.Site(app)
    g.enable(web, prefix='')
    return web
