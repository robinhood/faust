"""HTTP endpoint showing partition routing destinations."""
from .. import views
from .. import Request, Response

__all__ = ['TablesMetadata', 'TableMetadata', 'KeyMetadata']


class RouterView(views.View):
    # package to get templates from.
    package = 'faust.web.apps.router'


class TablesMetadata(RouterView):
    """List routes for all tables."""

    async def get(self, request: Request) -> Response:
        router = self.app.router
        return self.json(router.tables_metadata())


class TableMetadata(RouterView):
    """List route for specific table."""

    async def get(self, request: Request) -> Response:
        # FIXME request.match_info is an attribute of aiohttp.Request
        table_name = request.match_info['name']
        router = self.app.router
        return self.json(router.table_metadata(table_name))


class KeyMetadata(RouterView):
    """List information about key."""

    async def get(self, request: Request) -> Response:
        table_name = request.match_info['name']
        key = request.match_info['key']
        router = self.app.router
        return self.json(str(router.key_store(table_name, key)))


class Site(views.Site):
    """Router views."""

    views = {
        '/': TablesMetadata,
        '/{name}/': TableMetadata,
        '/{name}/{key}/': KeyMetadata,
    }
