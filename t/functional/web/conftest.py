import pytest
from faust.exceptions import SameNode
from faust.web.site import Website
from mode.utils.mocks import Mock

BASE_PORT = 6066


@pytest.fixture()
def site(app):
    return Website(app, port=BASE_PORT, driver='aiohttp://')


@pytest.fixture()
def client(loop, test_client, site):
    return test_client(site.web._app)


@pytest.fixture()
def router_same(app):
    app.router.route_req = Mock(name='app.router.route_req')
    app.router.route_req.side_effect = SameNode()
    return app.router
