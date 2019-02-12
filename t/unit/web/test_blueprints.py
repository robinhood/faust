from pathlib import Path
import pytest
from faust import web
from mode.utils.mocks import Mock


class test_Blueprint:

    @pytest.fixture()
    def bp(self):
        return web.Blueprint('test')

    def test_cache__with_prefix(self, *, bp):
        assert bp.cache(key_prefix='kp_').key_prefix == 'kp_'

    def test_apply_static(self, *, bp):
        bp.static('/static/', '/opt/static/', name='statici')
        assert bp.static_routes
        route = bp.static_routes[0]
        assert route.name == 'test.statici'
        app = Mock(name='app')
        bp.register(app, url_prefix='/foo/')
        app.web.add_static.assert_called_once_with(
            '/foo/static/', Path('/opt/static/'),
        )

    def test_apply_static__already_prefixed(self, *, bp):
        bp.static('/static/', '/opt/static/', name='test.statici')
        assert bp.static_routes
        route = bp.static_routes[0]
        assert route.name == 'test.statici'
