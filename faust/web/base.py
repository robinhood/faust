"""Base interface for Web server and views."""
import abc
import socket

from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    Type,
    Union,
)

from urllib.parse import quote
from mode import Service
from mode.utils.compat import want_str
from mode.utils.imports import SymbolArg, symbol_by_name
from yarl import URL

from faust.types import AppT
from faust.types.web import BlueprintT, ResourceOptions, View

__all__ = [
    'DEFAULT_BLUEPRINTS',
    'BlueprintManager',
    'Request',
    'Response',
    'Web',
]

_bytes = bytes

_BPArg = SymbolArg[BlueprintT]
_BPList = Iterable[Tuple[str, _BPArg]]

DEFAULT_BLUEPRINTS: _BPList = [
    ('/router', 'faust.web.apps.router:blueprint'),
    ('/table', 'faust.web.apps.tables.blueprint'),
]

PRODUCTION_BLUEPRINTS: _BPList = [
    ('', 'faust.web.apps.production_index:blueprint'),
]

DEBUG_BLUEPRINTS: _BPList = [
    ('/graph', 'faust.web.apps.graph:blueprint'),
    ('', 'faust.web.apps.stats:blueprint'),
]

CONTENT_SEPARATOR: bytes = b'\r\n\r\n'
HEADER_SEPARATOR: bytes = b'\r\n'
HEADER_KEY_VALUE_SEPARATOR: bytes = b': '


class Response:
    """Web server response and status."""

    @property
    @abc.abstractmethod
    def status(self) -> int:
        """Return the response status code."""
        ...

    @property
    @abc.abstractmethod
    def body(self) -> _bytes:
        """Return the response body as bytes."""
        ...

    @property
    @abc.abstractmethod
    def headers(self) -> MutableMapping:
        """Return mapping of response HTTP headers."""
        ...

    @property
    @abc.abstractmethod
    def content_length(self) -> Optional[int]:
        """Return the size of the response body."""
        ...

    @property
    @abc.abstractmethod
    def content_type(self) -> str:
        """Return the response content type."""
        ...

    @property
    @abc.abstractmethod
    def charset(self) -> Optional[str]:
        """Return the response character set."""
        ...

    @property
    @abc.abstractmethod
    def chunked(self) -> bool:
        """Return :const:`True` if response is chunked."""
        ...

    @property
    @abc.abstractmethod
    def compression(self) -> bool:
        """Return :const:`True` if the response body is compressed."""
        ...

    @property
    @abc.abstractmethod
    def keep_alive(self) -> Optional[bool]:
        """Return :const:`True` if HTTP keep-alive enabled."""
        ...

    @property
    @abc.abstractmethod
    def body_length(self) -> int:
        """Size of HTTP response body."""
        ...


class BlueprintManager:
    """Manager of all blueprints."""

    applied: bool

    _enabled: List[Tuple[str, _BPArg]]
    _active: MutableMapping[str, BlueprintT]

    def __init__(self, initial: _BPList = None) -> None:
        self.applied = False
        self._enabled = list(initial) if initial else []
        self._active = {}

    def add(self, prefix: str, blueprint: _BPArg) -> None:
        """Register blueprint with this app."""
        if self.applied:
            raise RuntimeError('Cannot add blueprints after server started')
        self._enabled.append((prefix, blueprint))

    def apply(self, web: 'Web') -> None:
        """Apply all blueprints."""
        if not self.applied:
            self.applied = True
            for prefix, blueprint in self._enabled:
                bp: BlueprintT = symbol_by_name(blueprint)
                self._apply_blueprint(web, prefix, bp)

    def _apply_blueprint(self,
                         web: 'Web',
                         prefix: str,
                         bp: BlueprintT) -> None:
        self._active[bp.name] = bp
        bp.register(web.app, url_prefix=prefix)
        bp.init_webserver(web)


class Web(Service):
    """Web server and HTTP interface."""

    default_blueprints: ClassVar[_BPList] = DEFAULT_BLUEPRINTS  # noqa: E704
    production_blueprints: ClassVar[_BPList] = PRODUCTION_BLUEPRINTS
    debug_blueprints: ClassVar[_BPList] = DEBUG_BLUEPRINTS

    app: AppT

    driver_version: str

    views: MutableMapping[str, View]
    reverse_names: MutableMapping[str, str]

    blueprints: BlueprintManager

    content_separator: ClassVar[bytes] = CONTENT_SEPARATOR
    header_separator: ClassVar[bytes] = HEADER_SEPARATOR
    header_key_value_separator: ClassVar[bytes] = HEADER_KEY_VALUE_SEPARATOR

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        self.views = {}
        self.reverse_names = {}
        blueprints = list(self.default_blueprints)
        if self.app.conf.debug:
            blueprints.extend(self.debug_blueprints)
        else:
            blueprints.extend(self.production_blueprints)
        self.blueprints = BlueprintManager(blueprints)
        Service.__init__(self, **kwargs)

    @abc.abstractmethod
    def text(self, value: str, *,
             content_type: str = None,
             status: int = 200,
             reason: str = None,
             headers: MutableMapping = None) -> Response:
        """Create text response, using "text/plain" content-type."""
        ...

    @abc.abstractmethod
    def html(self, value: str, *,
             content_type: str = None,
             status: int = 200,
             reason: str = None,
             headers: MutableMapping = None) -> Response:
        """Create HTML response from string, ``text/html`` content-type."""
        ...

    @abc.abstractmethod
    def json(self, value: Any, *,
             content_type: str = None,
             status: int = 200,
             reason: str = None,
             headers: MutableMapping = None) -> Response:
        """Create new JSON response.

        Accepts any JSON-serializable value and will automatically
        serialize it for you.

        The content-type is set to "application/json".
        """
        ...

    @abc.abstractmethod
    def bytes(self,
              value: _bytes,
              *,
              content_type: str = None,
              status: int = 200,
              reason: str = None,
              headers: MutableMapping = None) -> Response:
        """Create new ``bytes`` response - for binary data."""
        ...

    @abc.abstractmethod
    def bytes_to_response(self, s: _bytes) -> Response:
        """Deserialize HTTP response from byte string."""
        ...

    def _bytes_to_response(
            self, s: _bytes) -> Tuple[HTTPStatus, Mapping, _bytes]:
        status_code, _, payload = s.partition(self.content_separator)
        headers, _, body = payload.partition(self.content_separator)

        return (
            HTTPStatus(int(status_code)),
            dict(self._splitheader(h) for h in headers.splitlines()),
            body,
        )

    def _splitheader(self, header: _bytes) -> Tuple[str, str]:
        key, value = header.split(self.header_key_value_separator, 1)
        return want_str(key.strip()), want_str(value.strip())

    @abc.abstractmethod
    def response_to_bytes(self, response: Response) -> _bytes:
        """Serialize HTTP response into byte string."""
        ...

    def _response_to_bytes(
            self, status: int, headers: Mapping, body: _bytes) -> _bytes:
        return self.content_separator.join([
            str(status).encode(),
            self.content_separator.join([
                self._headers_serialize(headers),
                body,
            ]),
        ])

    def _headers_serialize(self, headers: Mapping) -> _bytes:
        return self.header_separator.join(
            self.header_key_value_separator.join([
                k if isinstance(k, _bytes) else k.encode('ascii'),
                v if isinstance(v, _bytes) else v.encode('latin-1'),
            ])
            for k, v in headers.items()
        )

    @abc.abstractmethod
    def route(self,
              pattern: str,
              handler: Callable,
              cors_options: Mapping[str, ResourceOptions] = None) -> None:
        """Add route for handler."""
        ...

    @abc.abstractmethod
    def add_static(self,
                   prefix: str,
                   path: Union[Path, str],
                   **kwargs: Any) -> None:
        """Add static route."""
        ...

    @abc.abstractmethod
    async def read_request_content(self, request: 'Request') -> _bytes:
        """Read HTTP body as bytes."""
        ...

    @abc.abstractmethod
    async def wsgi(self) -> Any:
        """WSGI entry point."""
        ...

    def add_view(self, view_cls: Type[View], *,
                 prefix: str = '',
                 cors_options: Mapping[str, ResourceOptions] = None) -> View:
        """Add route for view."""
        view: View = view_cls(self.app, self)
        path = prefix.rstrip('/') + '/' + view.view_path.lstrip('/')
        self.route(path, view, cors_options)
        self.views[path] = view
        self.reverse_names[view.view_name] = path
        return view

    def url_for(self, view_name: str, **kwargs: Any) -> str:
        """Get URL by view name.

        If the provided view name has associated URL parameters,
        those need to be passed in as kwargs, or a :exc:`TypeError`
        will be raised.
        """
        try:
            path = self.reverse_names[view_name]
        except KeyError:
            raise KeyError(f'No view with name {view_name!r} found')
        else:
            return path.format(**{
                k: self._quote_for_url(str(v)) for k, v in kwargs.items()})

    def _quote_for_url(self, value: str) -> str:
        return quote(value, safe='')  # disable '/' being safe by default

    def init_server(self) -> None:
        """Initialize and setup web server."""
        self.blueprints.apply(self)
        self.app.on_webserver_init(self)

    @property
    def url(self) -> URL:
        """Return the canonical URL to this worker (including port)."""
        canon = self.app.conf.canonical_url
        if canon.host == socket.gethostname():
            return URL(f'http://localhost:{self.app.conf.web_port}/')
        return self.app.conf.canonical_url


class Request(abc.ABC):
    """HTTP Request."""

    method: str
    headers: Mapping[str, str]
    url: URL
    rel_url: URL
    query_string: str
    keep_alive: bool
    body_exists: bool

    user: Any

    if_modified_since: Optional[datetime]
    if_unmodified_since: Optional[datetime]
    if_range: Optional[datetime]

    @abc.abstractmethod
    def can_read_body(self) -> bool:
        """Return :const:`True` if the request has a body."""
        ...

    @abc.abstractmethod
    async def read(self) -> bytes:
        """Read post data as bytes."""
        ...

    @abc.abstractmethod
    async def text(self) -> str:
        """Read post data as text."""
        ...

    @abc.abstractmethod
    async def json(self) -> Any:
        """Read post data and deserialize as JSON."""
        ...

    @abc.abstractmethod
    async def post(self) -> Mapping[str, str]:
        """Read post data."""
        ...

    @property
    @abc.abstractmethod
    def match_info(self) -> Mapping[str, str]:
        """Return match info from URL route as a mapping."""
        ...

    @property
    @abc.abstractmethod
    def query(self) -> Mapping[str, str]:
        """Return HTTP query parameters as a mapping."""
        ...

    @property
    @abc.abstractmethod
    def cookies(self) -> Mapping[str, Any]:
        """Return cookies as a mapping."""
        ...
