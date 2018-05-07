from unittest.mock import Mock
import pytest
from faust.types import TP
from mode.utils.futures import done_future


class test_AgentManager:

    def create_agent(self, name, topic_names=None):
        agent = Mock(name=name)
        agent.start.return_value = done_future()
        agent.stop.return_value = done_future()
        agent.restart.return_value = done_future()
        agent.on_partitions_revoked.return_value = done_future()
        agent.on_partitions_assigned.return_value = done_future()
        agent.get_topic_names.return_value = topic_names
        return agent

    @pytest.fixture()
    def agents(self, *, app):
        return app.agents

    @pytest.fixture()
    def agent1(self):
        return self.create_agent('agent1', ['t1'])

    @pytest.fixture()
    def agent2(self):
        return self.create_agent('agent2', ['t1', 't2', 't3'])

    @pytest.fixture()
    def many(self, *, agents, agent1, agent2):
        agents['foo'] = agent1
        agents['bar'] = agent2
        return agents

    def test_constructor(self, *, agents, app):
        assert agents.app is app
        assert agents.data == {}
        assert agents._by_topic == {}

    @pytest.mark.asyncio
    async def test_start(self, *, many):
        await many.start()
        for agent in many.values():
            agent.start.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_restart(self, *, many):
        await many.restart()
        for agent in many.values():
            agent.restart.assert_called_once_with()

    def test_service_reset(self, *, many):
        many.service_reset()
        for agent in many.values():
            agent.service_reset.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_stop(self, *, many):
        await many.stop()
        for agent in many.values():
            agent.cancel.assert_called_once_with()
            agent.stop.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, many, agent1, agent2):
        many._update_topic_index()
        await many.on_partitions_revoked({
            TP('t1', 0),
            TP('t1', 1),
            TP('t2', 3),
            TP('t2', 6),
            TP('t2', 7),
            TP('t3', 8),
            TP('t4', 9),
        })
        agent1.on_partitions_revoked.assert_called_once_with({
            TP('t1', 0), TP('t1', 1),
        })
        agent2.on_partitions_revoked.assert_called_once_with({
            TP('t1', 0),
            TP('t1', 1),
            TP('t2', 3),
            TP('t2', 6),
            TP('t2', 7),
            TP('t3', 8),
        })

    @pytest.mark.asyncio
    async def test_on_partitions_assigned(self, *, many, agent1, agent2):
        many._update_topic_index()
        await many.on_partitions_assigned({
            TP('t1', 0),
            TP('t1', 1),
            TP('t2', 3),
            TP('t2', 6),
            TP('t2', 7),
            TP('t3', 8),
            TP('t4', 9),
        })
        agent1.on_partitions_assigned.assert_called_once_with({
            TP('t1', 0), TP('t1', 1),
        })
        agent2.on_partitions_assigned.assert_called_once_with({
            TP('t1', 0),
            TP('t1', 1),
            TP('t2', 3),
            TP('t2', 6),
            TP('t2', 7),
            TP('t3', 8),
        })

    def test_update_topic_index(self, *, many, agent1, agent2):
        many._update_topic_index()
        assert set(many._by_topic['t1']) == {agent1, agent2}
        assert set(many._by_topic['t2']) == {agent2}
        assert set(many._by_topic['t3']) == {agent2}
