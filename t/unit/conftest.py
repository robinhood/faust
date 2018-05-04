from unittest.mock import Mock
import faust
import pytest
from mode.utils.futures import done_future


@pytest.fixture
def app():
    instance = faust.App('testid')
    instance.producer = Mock(name='producer')
    setup_producer(instance)
    instance.finalize()
    return instance


def setup_producer(app):
    app.producer.maybe_start.return_value = done_future()
    app.producer.start.return_value = done_future()
    app.producer.send.return_value = done_future()
    app.producer.send_and_wait.return_value = done_future()
