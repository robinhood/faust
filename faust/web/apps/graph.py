"""Web endpoint showing graph of running :pypi:`mode` services."""
import io
from faust import web

__all__ = ['Graph', 'blueprint']

blueprint = web.Blueprint('graph')


@blueprint.route('/', name='detail')
class Graph(web.View):
    """Render image from graph of running services."""

    async def get(self, request: web.Request) -> web.Response:
        try:
            import pydot
        except ImportError:
            return self.text('Please install pydot: pip install pydot',
                             status=500)
        o = io.StringIO()
        beacon = self.app.beacon.root or self.app.beacon
        beacon.as_graph().to_dot(o)
        graph, = pydot.graph_from_dot_data(o.getvalue())
        return self.bytes(graph.create_png(), content_type='image/png')
