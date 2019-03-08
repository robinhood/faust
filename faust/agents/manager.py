"""Agent manager."""
from collections import defaultdict
from typing import Any, Dict, MutableMapping, MutableSet, Set
from weakref import WeakSet
from mode import Service
from mode.utils.collections import ManagedUserDict
from mode.utils.compat import OrderedDict
from faust.types import AgentManagerT, AgentT, AppT
from faust.types.tuples import TP, tp_set_to_map
from faust.utils.tracing import traced_from_parent_span


class AgentManager(Service, AgentManagerT, ManagedUserDict):
    """Agent manager."""

    _by_topic: MutableMapping[str, MutableSet[AgentT]]

    def __init__(self, app: AppT,
                 **kwargs: Any) -> None:
        self.app = app
        self.data = OrderedDict()
        self._by_topic = defaultdict(WeakSet)
        Service.__init__(self, **kwargs)

    async def on_start(self) -> None:
        self.update_topic_index()
        for agent in self.values():
            await agent.maybe_start()

    def service_reset(self) -> None:
        [agent.service_reset() for agent in self.values()]
        super().service_reset()

    async def on_stop(self) -> None:
        for agent in self.values():
            await agent.stop()

    async def stop(self) -> None:
        # Cancel first so _execute_task sees we are not stopped.
        self.cancel()
        # Then stop the agents
        await super().stop()

    def cancel(self) -> None:
        [agent.cancel() for agent in self.values()]

    def update_topic_index(self) -> None:
        # keep mapping from topic name to set of agents.
        by_topic_index = self._by_topic
        for agent in self.values():
            for topic in agent.get_topic_names():
                by_topic_index[topic].add(agent)

    async def on_rebalance(self,
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        T = traced_from_parent_span()
        # for isolated_partitions agents we stop agents for revoked
        # partitions.
        for agent, tps in self._collect_agents_for_update(revoked).items():
            await T(agent.on_partitions_revoked)(tps)
        # for isolated_partitions agents we start agents for newly
        # assigned partitions
        for agent, tps in T(self._collect_agents_for_update)(
                newly_assigned).items():
            await T(agent.on_partitions_assigned)(tps)

    def _collect_agents_for_update(
            self, tps: Set[TP]) -> Dict[AgentT, Set[TP]]:
        by_agent: Dict[AgentT, Set[TP]] = defaultdict(set)
        for topic, tps in tp_set_to_map(tps).items():
            for agent in self._by_topic[topic]:
                by_agent[agent].update(tps)
        return by_agent
