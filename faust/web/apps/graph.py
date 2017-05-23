import io
from ..base import Request, Response, Web
from .. import views

__all__ = ['Graph', 'Site']


class Graph(views.View):

    async def get(self, web: Web, request: Request) -> Response:
        import pydot
        o = io.StringIO()
        beacon = self.app.beacon.root or self.app.beacon
        beacon.as_graph().to_dot(o)
        graph, = pydot.graph_from_dot_data(o.getvalue())
        return web.bytes(graph.create_png(), content_type='image/png')


class Site(views.Site):
    views = {
        '/': Graph,
    }
