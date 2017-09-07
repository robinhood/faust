from faust.web import views
from faust.web.base import Request, Response, Web

__all__ = ['TablesMetadata', 'TableMetadata', 'KeyMetadata']


class TablesMetadata(views.View):
    package = 'faust.web.apps.router'

    async def get(self, web: Web, request: Request) -> Response:
        router = self.app.router
        return web.json(router.tables_metadata())


class TableMetadata(views.View):
    package = 'faust.web.apps.router'

    async def get(self, web: Web, request: Request) -> Response:
        # FIXME request.match_info is an attribute of aiohttp.Request
        table_name = request.match_info['name']
        router = self.app.router
        return web.json(router.table_metadata(table_name))


class KeyMetadata(views.View):
    package = 'faust.web.apps.router'

    async def get(self, web: Web, request: Request) -> Response:
        table_name = request.match_info['name']
        key = request.match_info['key']
        router = self.app.router
        return web.json(router.key_store(table_name, key))


class Site(views.Site):
    views = {
        '/': TablesMetadata,
        '/{name}/': TableMetadata,
        '/{name}/{key}/': KeyMetadata,
    }
