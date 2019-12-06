"""LiveCheck :pypi:`aiohttp` integration."""
from contextlib import ExitStack
from types import SimpleNamespace
from typing import Any, List, Optional, no_type_check

import aiohttp
from aiohttp import web

from faust.livecheck.locals import current_test_stack
from faust.livecheck.models import TestExecution

__all__ = ['patch_all', 'patch_aiohttp_session', 'LiveCheckMiddleware']


def patch_all() -> None:
    """Patch all :pypi:`aiohttp` functions to integrate with LiveCheck."""
    patch_aiohttp_session()


def patch_aiohttp_session() -> None:
    """Patch :class:`aiohttp.ClientSession` to integrate with LiveCheck.

    If there is any currently active test, we will
    use that to forward LiveCheck HTTP headers to the new HTTP request.
    """
    from aiohttp import TraceConfig
    from aiohttp import client

    # monkeypatch to remove ridiculous "do not subclass" warning.
    def __init_subclass__() -> None:
        ...
    client.ClientSession.__init_subclass__ = __init_subclass__  # type: ignore

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
            super().__init__(
                trace_configs=self._faust_trace_configs(trace_configs),
                **kwargs)

        @no_type_check
        def _faust_trace_configs(
                self, configs: List[TraceConfig] = None) -> List[TraceConfig]:
            if configs is None:
                configs = []
            trace_config = aiohttp.TraceConfig()
            trace_config.on_request_start.append(_on_request_start)
            configs.append(trace_config)
            return configs

    client.ClientSession = ClientSession  # type: ignore


@web.middleware
class LiveCheckMiddleware:
    """LiveCheck support for :pypi:`aiohttp` web servers.

    This middleware is applied to all incoming web requests,
    and is used to extract LiveCheck HTTP headers.

    If the web request is configured with the correct set of LiveCheck
    headers, we will use that to set the "current test" context.
    """

    async def __call__(self, request: web.Request, handler: Any) -> Any:
        """Call to handle new web request."""
        related_test = TestExecution.from_headers(request.headers)
        with ExitStack() as stack:
            if related_test:
                stack.enter_context(current_test_stack.push(related_test))
            return await handler(request)
