import pydot
from . import views


class Graph(views.View):

    async def get(self, web, request):
        graph, = pydot.graph_from_dot_data(self.app.render_graph())
        return web.bytes(graph.create_png(), content_type='image/png')


class Site(views.Site):
    views = {
        '/': Graph,
    }
