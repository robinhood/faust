import pytest
from faust.exceptions import SameNode
from mode.utils.mocks import Mock


@pytest.fixture()
def web_client(loop, aiohttp_client, web):
    return aiohttp_client(web.web_app)


@pytest.fixture()
def router_same(app):
    app.router.route_req = Mock(name='app.router.route_req')
    app.router.route_req.side_effect = SameNode()
    return app.router
