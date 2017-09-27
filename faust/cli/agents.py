"""Program ``faust agents`` used to list agents.

.. program:: faust agents
"""
from operator import attrgetter
from typing import Optional, Sequence
import click
from .base import AppCommand
from ..types import AgentT


class agents(AppCommand):
    """List agents."""

    title = 'Agents'
    headers = ['name', 'topic', 'help']
    sortkey = attrgetter('name')

    options = [
        click.option('--local/--no-local',
                     help='Include agents using a local channel'),
    ]

    async def run(self, local: bool) -> None:
        self.say(self.tabulate(
            [self.agent_to_row(agent) for agent in self.agents(local=local)],
            headers=self.headers,
            title=self.title,
        ))

    def agents(self, *, local: bool = False) -> Sequence[AgentT]:
        return [
            agent
            for agent in sorted(self.app.agents.values(), key=self.sortkey)
            if self._maybe_topic(agent) or local
        ]

    def agent_to_row(self, agent: AgentT) -> Sequence[str]:
        return [
            self._bold_tail(self._name(agent)),
            self._topic(agent),
            self.colored('autoblack', self._help(agent)),
        ]

    def _name(self, agent: AgentT) -> str:
        name = agent.name
        if name.startswith(self.app.origin):
            name = name[len(self.app.origin) + 1:]
        return f'@{name}'

    def _bold_tail(self, text: str, *, sep: str = '.') -> str:
        head, _, tail = text.rpartition(sep)
        return sep.join([head, self.bold(tail)])

    def _maybe_topic(self, agent: AgentT) -> Optional[str]:
        try:
            return agent.channel.get_topic_name()
        except NotImplementedError:
            return None

    def _topic(self, agent: AgentT) -> str:
        return self._maybe_topic(agent) or '<LOCAL>'

    def _help(self, agent: AgentT) -> str:
        return agent.help or '<N/A>'
