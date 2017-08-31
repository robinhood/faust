from collections import defaultdict
from typing import Iterable, List, MutableMapping
from faust.types.topics import TopicPartition
from faust.web import views
from faust.web.base import Request, Response, Web

__all__ = ['Assignment', 'Stats', 'Site']


TPMap = MutableMapping[str, List[int]]


class Stats(views.View):
    package = 'faust.web.apps.stats'

    async def get(self, web: Web, request: Request) -> Response:
        return web.json({
            f'Sensor{i}': s.asdict()
            for i, s in enumerate(self.app.sensors)
        })


class Assignment(views.View):
    package = 'faust.web.apps.stats'

    @classmethod
    def _topic_grouped(
            cls,
            assignment: Iterable[TopicPartition]) -> TPMap:
        tps = defaultdict(list)
        for tp in assignment:
            tps[tp.topic].append(tp.partition)
        return dict(tps)

    async def get(self, web: Web, request: Request) -> Response:
        assignor = self.app.assignor
        return web.json({
            'actives': self._topic_grouped(assignor.assigned_actives()),
            'standbys': self._topic_grouped(assignor.assigned_standbys()),
        })


class TablesMetadata(views.View):
    package = 'faust.web.apps.stats'

    async def get(self, web: Web, request: Request) -> Response:
        return web.json(self.app.assignor.tables_metadata())


class TableMetadata(views.View):
    package = 'faust.web.apps.stats'

    async def get(self, web: Web, request: Request) -> Response:
        # FIXME request.match_info is an attribute of aiohttp.Request
        table_name = request.match_info['name']
        table = self.app.tables.get_table(table_name)
        if table is None:
            raise Exception
        return web.json(self.app.assignor.table_metadata(table))


class Site(views.Site):
    views = {
        '/': Stats,
        '/assignment/': Assignment,
        '/metadata/': TablesMetadata,
        '/metadata/{name}/': TableMetadata,
    }
