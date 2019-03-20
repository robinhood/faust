import pytest
from faust.web import Blueprint
from faust.web.base import BlueprintManager, Web
from mode.utils.mocks import Mock, patch
from yarl import URL


class test_BlueprintManager:

    @pytest.fixture()
    def manager(self):
        return BlueprintManager()

    def test_add(self, *, manager):
        bp1 = Blueprint('test1')
        manager.add('/v1/', bp1)
        assert manager._enabled
        assert manager._enabled[0] == ('/v1/', bp1)

    def test_add__already_applied(self, *, manager):
        bp1 = Blueprint('test1')
        manager.applied = True
        with pytest.raises(RuntimeError):
            manager.add('/v1', bp1)

    def test_apply(self, *, manager):
        web = Mock(name='web')
        bp1 = Mock(name='blueprint1')
        bp1.name = 'blueprint1'
        bp2 = Mock(name='blueprint2')
        bp2.name = 'blueprint2'

        manager.add('/v1/', bp1)
        manager.add('/v2/', bp2)
        manager.apply(web)

        assert manager._active['blueprint1'] is bp1
        assert manager._active['blueprint2'] is bp2

        bp1.register.assert_called_once_with(web.app, url_prefix='/v1/')
        bp2.register.assert_called_once_with(web.app, url_prefix='/v2/')
        bp1.init_webserver.assert_called_once_with(web)
        bp2.init_webserver.assert_called_once_with(web)

        assert manager.applied
        manager.apply(web)


class MyWeb(Web):

    def text(self, *args, **kwargs):
        ...

    def html(self, *args, **kwargs):
        ...

    def json(self, *args, **kwargs):
        ...

    def bytes(self, *args, **kwargs):
        ...

    def bytes_to_response(self, *args, **kwargs):
        ...

    def response_to_bytes(self, *args, **kwargs):
        ...

    def route(self, *args, **kwargs):
        ...

    def add_static(self, *args, **kwargs):
        ...

    async def read_request_content(self, *args, **kwargs):
        ...

    async def wsgi(self, *args, **kwargs):
        ...


class test_Web:

    @pytest.fixture()
    def web(self, *, app):
        return MyWeb(app)

    def test_url_for(self, *, web):
        web.reverse_names['test'] = '/foo/{bar}/'
        assert web.url_for(
            'test', bar='the quick/fox') == '/foo/the%20quick%2Ffox/'

    def test_url_for__not_found(self, *, web):
        with pytest.raises(KeyError):
            web.url_for('foo')

    def test_url__on_localhost(self, *, web, app):
        with patch('socket.gethostname') as gethostname:
            gethostname.return_value = 'foobar.example.com'
            app.conf.web_port = 3030
            app.conf.canonical_url = URL('http://foobar.example.com')
            assert web.url == URL('http://localhost:3030')

    def test_url__not_on_localhost(self, *, web, app):
        with patch('socket.gethostname') as gethostname:
            gethostname.return_value = 'foobar.example.com'
            app.conf.canonical_url = URL('http://xuzzy.example.com')
            assert web.url == URL('http://xuzzy.example.com')
