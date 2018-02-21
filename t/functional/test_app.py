import pytest
from faust import App
from yarl import URL


class test_settings:

    def test_app_broker(self):
        app = App('id', broker='foo://')
        assert app.conf.id == 'id'
        assert str(app.conf.broker) == 'foo:'
        assert isinstance(app.conf.broker, URL)
