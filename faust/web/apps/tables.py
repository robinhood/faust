"""HTTP endpoint showing partition routing destinations."""
from typing import Any, Mapping, Optional, Tuple, cast
from faust import web
from faust.app.router import SameNode
from faust.models import Record
from faust.types import K, TableT

__all__ = ['TableView', 'TableList', 'TableDetail', 'TableKeyDetail']


class TableInfo(Record, serializer='json', namespace='@TableInfo'):
    name: str
    help: str


class TableView(web.View):
    """Base class for table related views."""

    def table_json(self, table: TableT, **kwargs: Any) -> Mapping:
        return TableInfo(table.name, table.help).asdict()

    def get_table(self, name: str) -> Tuple[TableT,
                                            Optional[web.Response]]:
        try:
            return self.app.tables[name], None
        except KeyError:
            return (cast(TableT, None),
                    self.notfound('unknown table', name=name))

    def get_table_value(
            self,
            table: TableT,
            key: K) -> Tuple[Optional[Any], Optional[web.Response]]:
        try:
            return table[key], None
        except KeyError:
            return None, self.notfound(
                'key not found', table=table.name, key=key)


class TableList(TableView):
    """List available table names."""

    async def get(self, request: web.Request) -> web.Response:
        return self.json(
            [self.table_json(table) for table in self.app.tables.values()])


class TableDetail(TableView):
    """Get details for table by name."""

    async def get(self, request: web.Request) -> web.Response:
        # FIXME request.match_info is an attribute of aiohttp.Request
        name = request.match_info['name']
        table, error = self.get_table(name)
        if error is not None:
            return error
        return self.json(self.table_json(table))


class TableKeyDetail(TableView):
    """List information about key."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    async def get(self, request: web.Request) -> web.Response:
        name = request.match_info['name']
        key = request.match_info['key']
        router = self.app.router
        try:
            return await router.route_req(name, key, self.web, request)
        except SameNode:
            table, error = self.get_table(name)
            if error is not None:
                return error
            value, error = self.get_table_value(table, key)
            if error is not None:
                return error
            return self.json(value)


class Site(web.Site):
    """Router views."""

    views = {
        '/': TableList,
        '/{name}/': TableDetail,
        '/{name}/{key}/': TableKeyDetail,
    }
