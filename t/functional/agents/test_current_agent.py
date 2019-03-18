import pytest
from faust.agents import current_agent


@pytest.mark.asyncio
async def test_sets_current_agent(*, app, event_loop):
    assert current_agent() is None

    agent_body_executed = event_loop.create_future()

    @app.agent()
    async def agent(stream):
        agent_body_executed.set_result(True)
        assert current_agent() is agent
        async for item in stream:
            ...

    await agent.start()
    await agent_body_executed
    await agent.stop()
