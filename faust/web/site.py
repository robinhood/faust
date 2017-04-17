from .drivers.aiohttp import Web
from . import graph


def create_site(app):
    web = Web()
    g = graph.Site(app)
    g.enable(web, prefix='')
    return web
