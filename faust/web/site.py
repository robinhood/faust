from .drivers.aiohttp import Web
from ..types import AppT
from . import graph


def create_site(app: AppT) -> Web:
    web = Web()
    g = graph.Site(app)
    g.enable(web, prefix='')
    return web
