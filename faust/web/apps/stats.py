from ..base import Request, Response, Web
from .. import views

__all__ = ['Stats', 'Site']


class Stats(views.View):

    async def get(self, web: Web, request: Request) -> Response:
        return web.json({
            'Sensor{}'.format(i): s.asdict()
            for i, s in enumerate(self.app.sensors)
        })


class Site(views.Site):
    views = {
        '/': Stats,
    }
