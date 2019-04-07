from types import SimpleNamespace
import pytest
from mode.utils.mocks import Mock
from faust.livecheck.locals import current_test_stack
from faust.livecheck.patches.aiohttp import (
    LiveCheckMiddleware,
    patch_aiohttp_session,
)


@pytest.mark.asyncio
async def test_patch_aiohttp_session(*, execution):

    patch_aiohttp_session()
    from aiohttp.client import ClientSession
    trace_configs = []
    async with ClientSession(trace_configs=trace_configs):
        assert trace_configs
        config = trace_configs[-1]
        on_request_start = config.on_request_start[-1]

        session = Mock()
        ctx = SimpleNamespace()
        params = Mock()
        params.headers = {}

        assert current_test_stack.top is None

        await on_request_start(session, ctx, params)

        with current_test_stack.push(execution):
            await on_request_start(session, ctx, params)
            assert params.headers
            assert 'LiveCheck-Test-Id' in params.headers

    async with ClientSession(trace_configs=None):
        pass


@pytest.mark.asyncio
async def test_LiveCheckMiddleware(*, execution):
    request = Mock()
    request.headers = execution.as_headers()
    assert current_test_stack.top is None

    called = [False]

    async def handler(request):
        called[0] = True
        assert current_test_stack.top is not None

    await LiveCheckMiddleware()(request, handler)

    assert current_test_stack.top is None
    assert called[0]


@pytest.mark.asyncio
async def test_LiveCheckMiddleware__no_test(*, execution):
    request = Mock()
    request.headers = {}
    assert current_test_stack.top is None

    called = [False]

    async def handler(request):
        called[0] = True
        assert current_test_stack.top is None

    await LiveCheckMiddleware()(request, handler)

    assert current_test_stack.top is None
    assert called[0]
