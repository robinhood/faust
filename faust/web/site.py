from typing import Any
from .drivers.aiohttp import Web
from ..types import AppT
from .apps import graph
from .apps import stats

__all__ = ['create_site']


def create_site(app: AppT, **kwargs: Any) -> Web:
    web = Web(**kwargs)
    g = graph.Site(app)
    g.enable(web, prefix='/graph')
    s = stats.Site(app)
    s.enable(web, prefix='')
    return web
