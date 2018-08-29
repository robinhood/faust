import pytest
from mode.utils.mocks import AsyncMock, Mock
from faust.web import Request, View, Web
from faust.web.exceptions import MethodNotAllowed


@View.from_handler
async def foo(self, request):
    return self, request


class test_View:

    @pytest.fixture
    def web(self):
        return Mock(name='web', autospec=Web)

    @pytest.fixture
    def view(self, app, web):
        return foo(app, web)

    def test_from_handler(self):
        assert issubclass(foo, View)

    def test_from_handler__not_callable(self):
        with pytest.raises(TypeError):
            View.from_handler(1)

    def test_init(self, app, web, view):
        assert view.app is app
        assert view.web is web
        assert view.methods == {
            'get': view.get,
            'post': view.post,
            'patch': view.patch,
            'delete': view.delete,
            'put': view.put,
        }

    @pytest.mark.parametrize('method', [
        'GET', 'POST', 'PATCH', 'DELETE', 'PUT',
        'get', 'post', 'patch', 'delete', 'put',
    ])
    @pytest.mark.asyncio
    async def test_dispatch(self, method, *, view):
        request = Mock(name='request', autospec=Request)
        request.method = method
        request.match_info = {}
        handler = AsyncMock(name=method)
        view.methods[method.lower()] = handler
        assert await view(request) is handler.coro()
        handler.assert_called_once_with(request)

    @pytest.mark.asyncio
    async def test_get(self, view):
        req = Mock(name='request', autospec=Request)
        assert (await view.methods['get'](req)) == (view, req)

    @pytest.mark.asyncio
    async def test_interface_get(self, app, web):
        view = View(app, web)
        with pytest.raises(MethodNotAllowed):
            await view.get(Mock(name='request', autospec=Request))

    @pytest.mark.asyncio
    async def test_interface_post(self, view):
        with pytest.raises(MethodNotAllowed):
            await view.post(Mock(name='request', autospec=Request))

    @pytest.mark.asyncio
    async def test_interface_put(self, view):
        with pytest.raises(MethodNotAllowed):
            await view.put(Mock(name='request', autospec=Request))

    @pytest.mark.asyncio
    async def test_interface_patch(self, view):
        with pytest.raises(MethodNotAllowed):
            await view.patch(Mock(name='request', autospec=Request))

    @pytest.mark.asyncio
    async def test_interface_delete(self, view):
        with pytest.raises(MethodNotAllowed):
            await view.delete(Mock(name='request', autospec=Request))

    def test_text(self, view, web):
        response = view.text('foo', content_type='app/json', status=101)
        web.text.assert_called_once_with(
            'foo',
            content_type='app/json',
            status=101,
        )
        assert response is web.text()

    def test_html(self, view, web):
        response = view.html('foo', status=101)
        web.html.assert_called_once_with('foo', status=101)
        assert response is web.html()

    def test_json(self, view, web):
        response = view.json('foo', status=101)
        web.json.assert_called_once_with('foo', status=101)
        assert response is web.json()

    def test_bytes(self, view, web):
        response = view.bytes('foo', content_type='app/json', status=101)
        web.bytes.assert_called_once_with(
            'foo',
            content_type='app/json',
            status=101,
        )
        assert response is web.bytes()

    def test_route(self, view, web):
        handler = Mock(name='handler')
        res = view.route('pat', handler)
        web.route.assert_called_with('pat', handler)
        assert res is handler

    def test_error(self, view, web):
        response = view.error(303, 'foo', arg='bharg')
        web.json.assert_called_once_with(
            {'error': 'foo', 'arg': 'bharg'}, status=303)
        assert response is web.json()

    def test_notfound(self, view, web):
        response = view.notfound(arg='bharg')
        web.json.assert_called_once_with(
            {'error': 'Not Found', 'arg': 'bharg'}, status=404)
        assert response is web.json()
