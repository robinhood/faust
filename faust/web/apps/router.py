"""HTTP endpoint showing partition routing destinations."""
from faust import web
from faust.web.exceptions import ServiceUnavailable

__all__ = ['TableList', 'TableDetail', 'TableKeyDetail', 'blueprint']

blueprint = web.Blueprint('router')


@blueprint.route('/', name='list')
class TableList(web.View):
    """List routes for all tables."""

    async def get(self, request: web.Request) -> web.Response:
        router = self.app.router
        return self.json(router.tables_metadata())


@blueprint.route('/{name}/', name='detail')
class TableDetail(web.View):
    """List route for specific table."""

    async def get(self, request: web.Request, name: str) -> web.Response:
        router = self.app.router
        return self.json(router.table_metadata(name))


@blueprint.route('/{name}/{key}/', name='key-detail')
class TableKeyDetail(web.View):
    """List information about key."""

    async def get(self,
                  request: web.Request,
                  name: str,
                  key: str) -> web.Response:
        router = self.app.router
        try:
            dest_url = router.key_store(name, key)
        except KeyError:
            raise ServiceUnavailable()
        else:
            return self.json(str(dest_url))
