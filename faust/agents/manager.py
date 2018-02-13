"""Agent manager."""
from mode.utils.collections import FastUserDict
from mode.utils.compat import OrderedDict
from ..types import AgentManagerT, AppT


class AgentManager(AgentManagerT, FastUserDict):
    """Agent manager."""

    def __init__(self, app: AppT = None) -> None:
        self.app = app
        self.data = OrderedDict()

    async def restart(self) -> None:
        for agent in self.values():
            await agent.restart()
