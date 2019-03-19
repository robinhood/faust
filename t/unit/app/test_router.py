import pytest
from faust.app.router import Router
from faust.exceptions import SameNode
from faust.web.exceptions import ServiceUnavailable
from mode.utils.mocks import ANY, Mock
from yarl import URL


class test_Router:

    @pytest.fixture()
    def assignor(self, *, app):
        assignor = app.assignor = Mock(name='assignor')
        return assignor

    @pytest.fixture()
    def router(self, *, app, assignor):
        return Router(app)

    def test_constructor(self, *, router, app, assignor):
        assert router.app is app
        assert router._assignor is assignor

    def test_key_store(self, *, router, app, assignor):
        table = app.tables['foo'] = Mock(name='table')
        assert router.key_store('foo', 'k') is assignor.key_store.return_value
        assignor.key_store.assert_called_once_with(
            table.changelog_topic.get_topic_name(),
            table.changelog_topic.prepare_key.return_value,
        )

    def test_table_metadata(self, *, router, app, assignor):
        table = app.tables['foo'] = Mock(name='table')
        ret = router.table_metadata('foo')
        assert ret is assignor.table_metadata.return_value
        assignor.table_metadata.assert_called_once_with(
            table.changelog_topic.get_topic_name(),
        )

    def test_tables_metadata(self, *, router, assignor):
        res = router.tables_metadata()
        assert res is assignor.tables_metadata.return_value
        assignor.tables_metadata.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_route_req__unavail(self, *, router, app):
        web = Mock(name='web')
        request = Mock(name='request')
        app.router.key_store = Mock()
        app.router.key_store.side_effect = KeyError()
        with pytest.raises(ServiceUnavailable):
            await router.route_req('foo', 'k', web, request)

    @pytest.mark.asyncio
    async def test_route_req__same_node(self, *, router, app):
        app.conf.canonical_url = URL('http://example.com:8181')
        web = Mock(name='web')
        request = Mock(name='request')
        app.router.key_store = Mock()
        app.router.key_store.return_value = URL('http://example.com:8181')
        with pytest.raises(SameNode):
            await router.route_req('foo', 'k', web, request)

    @pytest.mark.asyncio
    @pytest.mark.http_session(text=b'foobar')
    async def test_route_req(self, *, router, app, mock_http_client):
        app.conf.canonical_url = URL('http://ge.example.com:8181')
        web = Mock(name='web')
        request = Mock(name='request')
        app.router.key_store = Mock()
        app.router.key_store.return_value = URL('http://el.example.com:8181')
        response = await router.route_req('foo', 'k', web, request)
        assert response is web.text.return_value
        web.text.assert_called_once_with(b'foobar', content_type=ANY)
