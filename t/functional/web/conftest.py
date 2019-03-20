import pytest
from faust.exceptions import SameNode
from mode.utils.mocks import Mock


@pytest.yield_fixture()
def web_client(loop, aiohttp_client, web):
    try:
        yield aiohttp_client(web.web_app)
    finally:
        # Cleanup threads started by loop.run_in_executor
        # at shutdown.
        if loop._default_executor is not None:
            loop._default_executor.shutdown()


@pytest.fixture()
def router_same(app):
    app.router.route_req = Mock(name='app.router.route_req')
    app.router.route_req.side_effect = SameNode()
    return app.router
