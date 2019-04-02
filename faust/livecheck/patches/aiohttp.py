from contextlib import ExitStack
from types import SimpleNamespace
from typing import Any, List, Optional
import aiohttp
from aiohttp import web
from faust.livecheck.locals import current_test_stack
from faust.livecheck.models import TestExecution

__all__ = ['patch_all', 'patch_aiohttp_session', 'LiveCheckMiddleware']


def patch_all() -> None:
    patch_aiohttp_session()


def patch_aiohttp_session() -> None:
    from aiohttp import TraceConfig
    from aiohttp import client

    # monkeypatch to remove ridiculous "do not subclass" warning.
    def __init_subclass__() -> None:
        ...
    client.ClientSession.__init_subclass__ = __init_subclass__

    async def _on_request_start(
            session: aiohttp.ClientSession,
            trace_config_ctx: SimpleNamespace,
            params: aiohttp.TraceRequestStartParams) -> None:
        test = current_test_stack.top
        if test is not None:
            params.headers.update(test.as_headers())

    class ClientSession(client.ClientSession):

        def __init__(self,
                     trace_configs: Optional[List[TraceConfig]] = None,
                     **kwargs: Any) -> None:
            if trace_configs is None:
                trace_configs = []
            trace_config = aiohttp.TraceConfig()
            trace_config.on_request_start.append(_on_request_start)
            trace_configs.append(trace_config)

            super().__init__(trace_configs=trace_configs, **kwargs)

    client.ClientSession = ClientSession


@web.middleware
class LiveCheckMiddleware:

    async def __call__(self, request: web.Request, handler: Any) -> Any:
        related_test = TestExecution.from_headers(request.headers)
        with ExitStack() as stack:
            if related_test:
                stack.enter_context(current_test_stack.push(related_test))
            return await handler(request)
