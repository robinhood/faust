"""Agent manager."""
from collections import defaultdict
from typing import Any, Dict, MutableMapping, MutableSet, Set
from weakref import WeakSet
from mode import Service
from mode.proxy import ServiceProxy
from mode.utils.collections import ManagedUserDict
from mode.utils.compat import OrderedDict
from mode.utils.objects import cached_property
from faust.types import AgentManagerT, AgentT, AppT
from faust.types.tuples import TP, tp_set_to_map


class AgentManager(ServiceProxy, AgentManagerT, ManagedUserDict):
    """Agent manager."""

    _by_topic: MutableMapping[str, MutableSet[AgentT]]

    def __init__(self, app: AppT,
                 **kwargs: Any) -> None:
        self.app = app
        self.data = OrderedDict()
        self._by_topic = defaultdict(WeakSet)

    @cached_property
    def _service(self) -> 'AgentManagerService':
        return AgentManagerService(self)

    def update_topic_index(self) -> None:
        # keep mapping from topic name to set of agents.
        by_topic_index = self._by_topic
        for agent in self.values():
            for topic in agent.get_topic_names():
                by_topic_index[topic].add(agent)

    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        # for isolated_partitions agents we stop agents for revoked
        # partitions.
        for agent, tps in self._collect_agents_for_update(revoked).items():
            await agent.on_partitions_revoked(tps)

    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        # for isolated_partitions agents we start agents for newly
        # assigned partitions
        for agent, tps in self._collect_agents_for_update(assigned).items():
            await agent.on_partitions_assigned(tps)

    def _collect_agents_for_update(
            self, tps: Set[TP]) -> Dict[AgentT, Set[TP]]:
        by_agent: Dict[AgentT, Set[TP]] = defaultdict(set)
        for topic, tps in tp_set_to_map(tps).items():
            for agent in self._by_topic[topic]:
                by_agent[agent].update(tps)
        return by_agent


class AgentManagerService(Service):

    def __init__(self, agents: AgentManager, **kwargs: Any) -> None:
        self.agents = agents
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        self.agents.update_topic_index()
        for agent in self.agents.values():
            await agent.maybe_start()

    def service_reset(self) -> None:
        [agent.service_reset() for agent in self.agents.values()]

    async def stop(self) -> None:
        # Cancel first so _execute_task sees we are not stopped.
        self.cancel()
        # Then stop the agents
        await super().stop()

    async def on_stop(self) -> None:
        for agent in self.agents.values():
            await agent.stop()

    def cancel(self) -> None:
        [agent.cancel() for agent in self.agents.values()]
