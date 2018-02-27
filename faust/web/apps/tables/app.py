"""HTTP endpoint showing partition routing destinations."""
from typing import Any, Mapping, Tuple
from faust.models import Record
from faust.router import SameNode
from faust.types import K, TableT
from faust.web import views
from faust.web.base import Request, Response

__all__ = ['TableView', 'TableList', 'TableDetail', 'TableKeyDetail']


class TableInfo(Record, serializer='json'):
    name: str
    help: str


class TableView(views.View):
    """Base class for table related views."""

    # package with templates
    package = 'faust.web.apps.tables'

    def table_json(self, table: TableT, **kwargs: Any) -> Mapping:
        return TableInfo(table.name, table.help).asdict()

    def get_table(self, name: str) -> Tuple[TableT, Response]:
        try:
            return self.app.tables[name], None
        except KeyError:
            return None, self.notfound('unknown table', name=name)

    def get_table_value(self,
                        table: TableT,
                        key: K) -> Tuple[Any, Response]:
        try:
            return table[key], None
        except KeyError:
            return None, self.notfound(
                'key not found', table=table.name, key=key)


class TableList(TableView):
    """List available table names."""

    async def get(self, request: Request) -> Response:
        return self.json([
            self.table_json(table)
            for table in self.app.tables.values()
        ])


class TableDetail(TableView):
    """Get details for table by name."""

    async def get(self, request: Request) -> Response:
        # FIXME request.match_info is an attribute of aiohttp.Request
        name = request.match_info['name']
        table, error = self.get_table(name)
        return error if error else self.json(self.table_json(table))


class TableKeyDetail(TableView):
    """List information about key."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    async def get(self, request: Request) -> Response:
        name = request.match_info['name']
        key = request.match_info['key']
        try:
            return await self.app.router.route_req(
                name, key, self.web, request)
        except SameNode:
            table, error = self.get_table(name)
            if error:
                return error
            value, error = self.get_table_value(table, key)
            if error:
                return error
            return self.json(value)


class Site(views.Site):
    """Router views."""

    views = {
        '/': TableList,
        '/{name}/': TableDetail,
        '/{name}/{key}/': TableKeyDetail,
    }
