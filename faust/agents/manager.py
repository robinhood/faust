"""Agent manager."""
from mode.utils.collections import FastUserDict
from mode.utils.compat import OrderedDict
from faust.types import AgentManagerT, AppT


class AgentManager(AgentManagerT, FastUserDict):
    """Agent manager."""

    def __init__(self, app: AppT = None) -> None:
        self.app = app
        self.data = OrderedDict()

    async def start(self) -> None:
        [await agent.start() for agent in self.values()]

    async def restart(self) -> None:
        [await agent.restart() for agent in self.values()]

    def service_reset(self) -> None:
        [agent.service_reset() for agent in self.values()]

    async def stop(self) -> None:
        # Cancel first so _execute_task sees we are not stopped.
        self.cancel()
        # Then stop the agents
        [await agent.stop() for agent in self.values()]

    def cancel(self) -> None:
        [agent.cancel() for agent in self.values()]
