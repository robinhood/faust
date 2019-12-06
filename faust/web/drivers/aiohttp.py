"""Web driver using :pypi:`aiohttp`."""
from pathlib import Path
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    Optional,
    Union,
    cast,
)

import aiohttp_cors
from aiohttp import __version__ as aiohttp_version
from aiohttp.web import (
    AppRunner,
    Application,
    BaseSite,
    Request,
    Response,
    TCPSite,
    UnixSite,
)
from aiohttp.payload import Payload
from aiohttp_cors import CorsConfig, ResourceOptions
from faust.types import AppT
from faust.utils import json as _json
from faust.web import base
from faust.types.web import ResourceOptions as _ResourceOptions
from mode import Service
from mode.threads import ServiceThread

__all__ = ['Web']

_bytes = bytes

NON_OPTIONS_METHODS = frozenset({'GET', 'PUT', 'POST', 'DELETE'})


def _prepare_cors_options(
        opts: Mapping[str, Any]) -> Mapping[str, ResourceOptions]:
    return {k: _faust_to_aiohttp_options(v) for k, v in opts.items()}


def _faust_to_aiohttp_options(
        opts: ResourceOptions) -> ResourceOptions:
    if isinstance(opts, _ResourceOptions):
        return ResourceOptions(**opts._asdict())
    return opts


class ServerThread(ServiceThread):
    """A web server running in a dedicated thread."""

    def __init__(self, web: 'Web', **kwargs: Any) -> None:
        self.web = web
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        """Call in parent thread when the service thread is starting."""
        await self.web.start_server()

    async def on_thread_stop(self) -> None:
        """Call in thread when the service stops."""
        # on_stop() executes in parent thread, on_thread_stop in the thread.
        await self.web.stop_server()


class Server(Service):
    """Web server service."""

    def __init__(self, web: 'Web', **kwargs: Any) -> None:
        self.web = web
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        """Call when the web server starts."""
        await self.web.start_server()

    async def on_stop(self) -> None:
        """Call when the web server stops."""
        await self.web.stop_server()


class Web(base.Web):
    """Web server and framework implementation using :pypi:`aiohttp`."""

    driver_version = f'aiohttp={aiohttp_version}'
    handler_shutdown_timeout: float = 60.0
    cors_options: Mapping[str, ResourceOptions]

    #: We serve the web server in a separate thread (and separate event loop).
    _thread: Optional[Service] = None

    _transport_handlers: Mapping[str, Callable[[], BaseSite]]
    _cors: Optional[CorsConfig] = None

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(app, **kwargs)
        self.web_app: Application = Application()
        self.cors_options = _prepare_cors_options(
            app.conf.web_cors_options or {})
        self._runner: AppRunner = AppRunner(self.web_app, access_log=None)
        self._transport_handlers = {
            'tcp': self._new_transport_tcp,
            'unix': self._new_transport_unix,
        }

    @property
    def cors(self) -> CorsConfig:
        """Return CORS config object."""
        if self._cors is None:
            self._cors = aiohttp_cors.setup(
                self.web_app, defaults=self.cors_options)
        return self._cors

    async def on_start(self) -> None:
        """Call when the embedded web server starts.

        Only used for `faust worker`, not when using :meth:`wsgi`.
        """
        cors = self.cors
        assert cors
        self.init_server()
        server_cls = ServerThread if self.app.conf.web_in_thread else Server
        self._thread = server_cls(self, loop=self.loop, beacon=self.beacon)
        self.add_dependency(self._thread)

    async def wsgi(self) -> Any:
        """Call WSGI handler.

        Used by :pypi:`gunicorn` and other WSGI compatible hosts
        to access the Faust web entry point.
        """
        self.init_server()
        return self.web_app

    def text(self, value: str, *,
             content_type: str = None,
             status: int = 200,
             reason: str = None,
             headers: MutableMapping = None) -> base.Response:
        """Create text response, using "text/plain" content-type."""
        response = Response(
            text=value,
            content_type=content_type,
            status=status,
            reason=reason,
            headers=headers,
        )
        return cast(base.Response, response)

    def html(self, value: str, *,
             content_type: str = None,
             status: int = 200,
             reason: str = None,
             headers: MutableMapping = None) -> base.Response:
        """Create HTML response from string, ``text/html`` content-type."""
        return self.text(
            value,
            status=status,
            content_type=content_type or 'text/html',
            reason=reason,
            headers=headers,
        )

    def json(self, value: Any, *,
             content_type: str = None,
             status: int = 200,
             reason: str = None,
             headers: MutableMapping = None) -> Any:
        """Create new JSON response.

        Accepts any JSON-serializable value and will automatically
        serialize it for you.

        The content-type is set to "application/json".
        """
        ctype = content_type or 'application/json'
        payload: Any = _json.dumps(value)
        # normal json returns str, orjson returns bytes
        if isinstance(payload, bytes):
            return self.bytes(
                payload,
                content_type=ctype,
                status=status,
                reason=reason,
                headers=headers,
            )
        else:
            return self.text(
                payload,
                content_type=ctype,
                status=status,
                reason=reason,
                headers=headers,
            )

    def bytes(self,
              value: _bytes,
              *,
              content_type: str = None,
              status: int = 200,
              reason: str = None,
              headers: MutableMapping = None) -> base.Response:
        """Create new ``bytes`` response - for binary data."""
        response = Response(
            body=value,
            content_type=content_type,
            status=status,
            reason=reason,
            headers=headers,
        )
        return cast(base.Response, response)

    async def read_request_content(self, request: base.Request) -> _bytes:
        """Return the request body as bytes."""
        return await cast(Request, request).content.read()

    def route(self,
              pattern: str,
              handler: Callable,
              cors_options: Mapping[str, ResourceOptions] = None) -> None:
        """Add route for web view or handler."""
        if cors_options or self.cors_options:
            async_handler = self._wrap_into_asyncdef(handler)
            for method in NON_OPTIONS_METHODS:
                r = self.web_app.router.add_route(
                    method, pattern, async_handler)
                self.cors.add(r, _prepare_cors_options(cors_options or {}))
        else:
            self.web_app.router.add_route(
                '*', pattern, self._wrap_into_asyncdef(handler))

    def _wrap_into_asyncdef(self, handler: Callable) -> Callable:
        # get rid of pesky "DeprecationWarning: Bare functions are
        # deprecated, use async ones" warnings.
        # The handler is actually a class that defines `async def __call__`
        # but aiohttp doesn't recognize it as such and emits the warning.
        # To avoid that we just wrap it in an `async def` function
        async def _dispatch(request: base.Request) -> base.Response:
            return await handler(request)
        return _dispatch

    def add_static(self,
                   prefix: str,
                   path: Union[Path, str],
                   **kwargs: Any) -> None:
        """Add route for static assets."""
        self.web_app.router.add_static(prefix, str(path), **kwargs)

    def bytes_to_response(self, s: _bytes) -> base.Response:
        """Deserialize byte string back into a response object."""
        status, headers, body = self._bytes_to_response(s)
        response = Response(
            body=body,
            status=status,
            headers=headers,
        )
        return cast(base.Response, response)

    def response_to_bytes(self, response: base.Response) -> _bytes:
        """Convert response to serializable byte string.

        The result is a byte string that can be deserialized
        using :meth:`bytes_to_response`.
        """
        resp = cast(Response, response)
        if resp.body is None:
            body = b''
        elif isinstance(resp.body, Payload):
            raise NotImplementedError('Does not support Payload')
        else:
            body = resp.body
        return self._response_to_bytes(
            resp.status,
            resp.headers,
            body,
        )

    def _create_site(self) -> BaseSite:
        return self._new_transport(self.app.conf.web_transport.scheme)

    def _new_transport(self, type_: str) -> BaseSite:
        return self._transport_handlers[type_]()

    def _new_transport_tcp(self) -> BaseSite:
        return TCPSite(
            self._runner,
            self.app.conf.web_bind,
            self.app.conf.web_port,
        )

    def _new_transport_unix(self) -> BaseSite:
        return UnixSite(
            self._runner,
            self.app.conf.web_transport.path,
        )

    async def start_server(self) -> None:
        """Start the web server."""
        await self._runner.setup()
        site = self._create_site()
        await site.start()

    async def stop_server(self) -> None:
        """Stop the web server."""
        if self._runner:
            await self._runner.cleanup()
        await self._cleanup_app()

    async def _cleanup_app(self) -> None:
        if self.web_app is not None:
            self.log.info('Cleanup')
            await self.web_app.cleanup()

    @property
    def _app(self) -> Application:
        # XXX compat alias
        return self.web_app
