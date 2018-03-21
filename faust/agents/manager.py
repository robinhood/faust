"""Agent manager."""
from collections import defaultdict
from typing import MutableMapping, MutableSet, Set
from weakref import WeakSet
from mode.utils.collections import ManagedUserDict
from mode.utils.compat import OrderedDict
from faust.types import AgentManagerT, AgentT, AppT
from faust.types.tuples import TP, tp_set_to_map


class AgentManager(AgentManagerT, ManagedUserDict):
    """Agent manager."""

    _by_topic: MutableMapping[str, MutableSet[AgentT]]

    def __init__(self, app: AppT = None) -> None:
        self.app = app
        self.data = OrderedDict()
        self._by_topic = defaultdict(WeakSet)

    def on_key_set(self, key: str, value: AgentT) -> None:
        # keep mapping from topic name to set of agents.
        for topic in value.get_topic_names():
            self._by_topic[topic].add(value)

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

    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        # for isolated_partitions agents we stop agents for revoked
        # partitions.
        for topic, tps in tp_set_to_map(revoked).items():
            for agent in self._by_topic[topic]:
                await agent.on_partitions_revoked(tps)

    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        # for isolated_partitions agents we start agents for newly
        # assigned partitions
        for topic, tps in tp_set_to_map(assigned).items():
            for agent in self._by_topic[topic]:
                await agent.on_partitions_assigned(tps)
