"""HTTP endpoint showing statistics from the Faust monitor."""
from typing import List, MutableMapping
from faust import web

__all__ = ['Index', 'blueprint']

TPMap = MutableMapping[str, List[int]]


blueprint = web.Blueprint('production_index')


@blueprint.route('/', name='index')
class Index(web.View):
    """Dummy Index."""

    async def get(self, request: web.Request) -> web.Response:
        """Simple status page."""
        return self.json({'status': 'OK'})
