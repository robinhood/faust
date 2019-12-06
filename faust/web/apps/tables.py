"""HTTP endpoint showing partition routing destinations."""
from typing import Any, Mapping, cast
from faust import web
from faust.app.router import SameNode
from faust.models import Record
from faust.types import K, TableT, V

__all__ = [
    'TableView',
    'TableList',
    'TableDetail',
    'TableKeyDetail',
    'blueprint',
]


blueprint = web.Blueprint('tables')


class TableInfo(Record, serializer='json', namespace='@TableInfo'):
    name: str
    help: str


class TableView(web.View):
    """Base class for table related views."""

    def table_json(self, table: TableT, **kwargs: Any) -> Mapping:
        """Return table info as JSON serializable object."""
        return TableInfo(table.name, table.help).asdict()

    def get_table_or_404(self, name: str) -> TableT:
        """Find table by name, or raise NotFound if not found."""
        try:
            return cast(TableT, self.app.tables[name])
        except KeyError:
            raise self.NotFound('unknown table', name=name)

    def get_table_value_or_404(self, table: TableT, key: K) -> V:
        """Get value from table by key, or raise NotFound if not found."""
        try:
            return table[key]
        except KeyError:
            raise self.NotFound(f'key not found', key=key, table=table.name)


@blueprint.route('/', name='list')
class TableList(TableView):
    """List available table names."""

    async def get(self, request: web.Request) -> web.Response:
        """Return JSON response with a list of available table names."""
        return self.json(
            [self.table_json(table) for table in self.app.tables.values()])


@blueprint.route('/{name}/', name='detail')
class TableDetail(TableView):
    """Get details for table by name."""

    async def get(self, request: web.Request, name: str) -> web.Response:
        """Return JSON response with table information."""
        table = self.get_table_or_404(name)
        return self.json(self.table_json(table))


@blueprint.route('/{name}/{key}/', name='key-detail')
class TableKeyDetail(TableView):
    """List information about key."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    async def get(self,
                  request: web.Request,
                  name: str,
                  key: str) -> web.Response:
        """Look up value in table by key."""
        router = self.app.router
        try:
            return await router.route_req(name, key, self.web, request)
        except SameNode:
            table = self.get_table_or_404(name)
            value = self.get_table_value_or_404(table, key)
            return self.json(value)
