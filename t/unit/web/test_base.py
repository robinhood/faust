import pytest
from faust.web.base import Request, Web


class test_Web:

    @pytest.fixture
    def web(self, *, app):
        return Web(app, port=8080, bind='localhost')

    def test_text(self, *, web):
        web.text('foo')

    def test_html(self, *, web):
        web.html('foo')

    def test_json(self, *, web):
        web.json('foo')

    def test_bytes(self, *, web):
        web.bytes('foo')

    def test_route(self, *, web):
        web.route('foo', lambda: None)


class test_Request:

    def test_match_info(self):
        Request().match_info

    def test_query(self):
        Request().query
