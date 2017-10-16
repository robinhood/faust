"""Web endpoint showing graph of running :pypi:`mode` services."""
import io
from .. import views
from ..base import Request, Response, Web

__all__ = ['Graph', 'Site']


class Graph(views.View):
    """Render image from graph of running services."""

    async def get(self, web: Web, request: Request) -> Response:
        import pydot
        o = io.StringIO()
        beacon = self.app.beacon.root or self.app.beacon
        beacon.as_graph().to_dot(o)
        graph, = pydot.graph_from_dot_data(o.getvalue())
        return web.bytes(graph.create_png(), content_type='image/png')


class Site(views.Site):
    """Graph views."""

    views = {
        '/': Graph,
    }
