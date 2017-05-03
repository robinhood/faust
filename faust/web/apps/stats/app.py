from faust.web import views
from faust.web.base import Request, Response, Web

__all__ = ['Stats', 'Site']


class Stats(views.View):
    package = 'faust.web.apps.stats'

    async def get(self, web: Web, request: Request) -> Response:
        return web.json({
            'Sensor{}'.format(i): s.asdict()
            for i, s in enumerate(self.app.sensors)
        })


class Site(views.Site):
    views = {
        '/': Stats,
    }
