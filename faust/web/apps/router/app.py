from faust.web import views
from faust.web.base import Request, Response, Web

__all__ = ['TablesMetadata', 'TableMetadata', 'KeyMetadata']


class TablesMetadata(views.View):
    package = 'faust.web.apps.router'

    async def get(self, web: Web, request: Request) -> Response:
        return web.json(self.app.assignor.tables_metadata())


class TableMetadata(views.View):
    package = 'faust.web.apps.router'

    async def get(self, web: Web, request: Request) -> Response:
        # FIXME request.match_info is an attribute of aiohttp.Request
        table_name = request.match_info['name']
        table = self.app.tables.get_table(table_name)
        if table is None:
            raise Exception
        return web.json(self.app.assignor.table_metadata(table))


class KeyMetadata(views.View):
    package = 'faust.web.apps.router'

    async def get(self, web: Web, request: Request) -> Response:
        table_name = request.match_info['name']
        key = request.match_info['key']
        table = self.app.tables.get_table(table_name)
        if table is None:
            raise Exception
        return web.json(self.app.assignor.key_store(table, key))


class Site(views.Site):
    views = {
        '/': TablesMetadata,
        '/{name}/': TableMetadata,
        '/{name}/{key}/': KeyMetadata,
    }
