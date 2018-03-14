"""HTTP endpoint showing partition routing destinations."""
from faust import web

__all__ = ['TablesMetadata', 'TableMetadata', 'KeyMetadata']


class TablesMetadata(web.View):
    """List routes for all tables."""

    async def get(self, request: web.Request) -> web.Response:
        router = self.app.router
        return self.json(router.tables_metadata())


class TableMetadata(web.View):
    """List route for specific table."""

    async def get(self, request: web.Request) -> web.Response:
        # FIXME request.match_info is an attribute of aiohttp.Request
        table_name = request.match_info['name']
        router = self.app.router
        return self.json(router.table_metadata(table_name))


class KeyMetadata(web.View):
    """List information about key."""

    async def get(self, request: web.Request) -> web.Response:
        table_name = request.match_info['name']
        key = request.match_info['key']
        router = self.app.router
        return self.json(str(router.key_store(table_name, key)))


class Site(web.Site):
    """Router views."""

    views = {
        '/': TablesMetadata,
        '/{name}/': TableMetadata,
        '/{name}/{key}/': KeyMetadata,
    }
