import pydot
from .base import Request, Response, Web
from . import views


class Graph(views.View):

    async def get(self, web: Web, request: Request) -> Response:
        graph, = pydot.graph_from_dot_data(self.app.render_graph())
        return web.bytes(graph.create_png(), content_type='image/png')


class Site(views.Site):
    views = {
        '/': Graph,
    }
