import pytest
from faust.exceptions import SameNode
from mode.utils.mocks import Mock


@pytest.fixture()
def client(loop, test_client, web):
    return test_client(web._app)


@pytest.fixture()
def router_same(app):
    app.router.route_req = Mock(name='app.router.route_req')
    app.router.route_req.side_effect = SameNode()
    return app.router


@pytest.fixture()
def web_client(aiohttp_client):
    # an alias, just in case we want to swap to another framework later.
    return aiohttp_client
