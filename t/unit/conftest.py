import os
import faust
import pytest
from faust.transport.producer import Producer
from faust.utils.tracing import set_current_span
from mode.utils.mocks import AsyncMock, Mock


@pytest.fixture()
def app(event_loop):
    os.environ.pop('F_DATADIR', None)
    os.environ.pop('FAUST_DATADIR', None)
    os.environ.pop('F_WORKDIR', None)
    os.environ.pop('FAUST_WORKDIR', None)
    instance = faust.App('testid')
    instance.producer = Mock(
        name='producer',
        autospec=Producer,
        maybe_start=AsyncMock(),
        start=AsyncMock(),
        send=AsyncMock(),
        send_and_wait=AsyncMock(),
    )
    instance.finalize()
    set_current_span(None)
    return instance


@pytest.fixture()
def web(app):
    return app.web
