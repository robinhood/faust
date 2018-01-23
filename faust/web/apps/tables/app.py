"""HTTP endpoint showing partition routing destinations."""
from typing import Any
from faust.router import SameNode
from faust.web import views
from faust.web.base import Request, Response, Web

__all__ = ['TableView', 'TableList', 'TableDetail', 'TableKeyDetail']


class TableView(views.View):
    # package with templates
    package = 'faust.web.apps.tables'


class TableList(TableView):
    """List available table names."""

    async def get(self, web: Web, request: Request) -> Response:
        return web.json([
            {'table': table.name, 'help': table.help}
            for table in self.app.tables.values()
        ])


class TableDetail(TableView):
    """Get details for table by name."""

    async def get(self, web: Web, request: Request) -> Response:
        # FIXME request.match_info is an attribute of aiohttp.Request
        table_name = request.match_info['name']
        return web.json({
            'table': table_name,
            'help': self.app.tables[table_name].help,
        })


class TableKeyDetail(TableView):
    """List information about key."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    async def get(self, web: Web, request: Request) -> Response:
        table = request.match_info['name']
        key = request.match_info['key']
        try:
            return await self.app.router.route_req(table, key, web, request)
        except SameNode:
            return web.json(self.app.tables[table][key])


class Site(views.Site):
    """Router views."""

    views = {
        '/': TableList,
        '/{name}/': TableDetail,
        '/{name}/{key}/': TableKeyDetail,
    }
