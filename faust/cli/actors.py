"""Program ``faust actors`` used to list actors.

.. program:: faust actors
"""
from operator import attrgetter
from typing import Optional, Sequence
import click
from .base import AppCommand
from ..types import ActorT


class actors(AppCommand):
    """List actors."""

    title = 'Actors'
    headers = ['name', 'channel', 'help']
    sortkey = attrgetter('name')

    options = [
        click.option('--local/--no-local',
                     help='Include actors using a local channel'),
    ]

    async def run(self, local: bool) -> None:
        self.say(self.tabulate(
            [self.actor_to_row(actor) for actor in self.actors(local=local)],
            headers=self.headers,
            title=self.title,
        ))

    def actors(self, *, local: bool = False) -> Sequence[ActorT]:
        return [
            actor
            for actor in sorted(self.app.actors.values(), key=self.sortkey)
            if self._maybe_topic(actor) or local
        ]

    def actor_to_row(self, actor: ActorT) -> Sequence[str]:
        return [
            self._name(actor),
            self._topic(actor),
            self._help(actor),
        ]

    def _name(self, actor: ActorT) -> str:
        name = actor.name
        if name.startswith(self.app.origin):
            name = name[len(self.app.origin) + 1:]
        return f'@{name}'

    def _maybe_topic(self, actor: ActorT) -> Optional[str]:
        try:
            return actor.channel.get_topic_name()
        except NotImplementedError:
            return None

    def _topic(self, actor: ActorT) -> str:
        return self._maybe_topic(actor) or '<LOCAL>'

    def _help(self, actor: ActorT) -> str:
        return actor.help or '<N/A>'
