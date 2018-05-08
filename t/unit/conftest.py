import faust
import pytest
from mode.utils.mocks import AsyncMock, Mock


@pytest.fixture
def app():
    instance = faust.App('testid')
    instance.producer = Mock(
        name='producer',
        maybe_start=AsyncMock(),
        start=AsyncMock(),
        send=AsyncMock(),
        send_and_wait=AsyncMock(),
    )
    instance.finalize()
    return instance
