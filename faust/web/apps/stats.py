"""HTTP endpoint showing statistics from the Faust monitor."""
from collections import defaultdict
from typing import List, MutableMapping, Set
from faust import web
from faust.types.tuples import TP

__all__ = ['Assignment', 'Stats', 'Site']

TPMap = MutableMapping[str, List[int]]


class Stats(web.View):
    """Monitor statistics."""

    async def get(self, request: web.Request) -> web.Response:
        return self.json(
            {f'Sensor{i}': s.asdict()
             for i, s in enumerate(self.app.sensors)})


class Assignment(web.View):
    """Cluster assignment information."""

    @classmethod
    def _topic_grouped(cls, assignment: Set[TP]) -> TPMap:
        tps: MutableMapping[str, List[int]] = defaultdict(list)
        for tp in sorted(assignment):
            tps[tp.topic].append(tp.partition)
        return dict(tps)

    async def get(self, request: web.Request) -> web.Response:
        assignor = self.app.assignor
        return self.json({
            'actives': self._topic_grouped(assignor.assigned_actives()),
            'standbys': self._topic_grouped(assignor.assigned_standbys()),
        })


class Site(web.Site):
    """Statistics views."""

    views = {
        '/': Stats,
        '/assignment/': Assignment,
    }
