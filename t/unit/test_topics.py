from unittest.mock import Mock
import pytest


class test_Topic:

    @pytest.fixture
    def topic(self, *, app):
        return app.topic('foo')

    def test_on_published(self, *, topic):
        fut = Mock(name='fut')
        message = Mock(name='message')
        topic._on_published(fut, message)
        fut.result.assert_called_once_with()
        message.set_result.assert_called_once_with(fut.result())
        message.message.callback.assert_called_once_with(message)
        message.message.callback = None
        topic._on_published(fut, message)

    def test_aiter_when_iterator(self, *, topic):
        topic.is_iterator = True
        assert topic.__aiter__() is topic
