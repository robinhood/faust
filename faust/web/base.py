"""Base interface for Web server and views."""
import abc
import socket
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    Iterable,
    List,
    MutableMapping,
    Tuple,
    Type,
    Union,
)
from urllib.parse import quote
from mode import Service
from mode.utils.imports import SymbolArg, symbol_by_name
from yarl import URL
from faust.types import AppT
from faust.types.web import BlueprintT, View

__all__ = [
    'DEFAULT_BLUEPRINTS',
    'BlueprintManager',
    'Request',
    'Response',
    'Web',
]

_bytes = bytes

_BPList = Iterable[Tuple[str, SymbolArg[Type[BlueprintT]]]]

DEFAULT_BLUEPRINTS: _BPList = [
    ('/graph', 'faust.web.apps.graph:blueprint'),
    ('', 'faust.web.apps.stats:blueprint'),
    ('/router', 'faust.web.apps.router:blueprint'),
    ('/table', 'faust.web.apps.tables.blueprint'),
]


class Response:
    """Web server response and status."""

    @property
    @abc.abstractmethod
    def status(self) -> int:
        ...

    @property
    @abc.abstractmethod
    def body(self) -> _bytes:
        ...


class BlueprintManager:
    applied: bool

    _enabled: List[Tuple[str, str]]
    _active: MutableMapping[str, BlueprintT]

    def __init__(self, initial: _BPList = None) -> None:
        self.applied = False
        self._enabled = list(initial) if initial else []
        self._active = {}

    def add(self, prefix: str, blueprint: SymbolArg[Type[BlueprintT]]) -> None:
        if self.applied:
            raise RuntimeError('Cannot add blueprints after server started')
        self._enabled.append((prefix, blueprint))

    def apply(self, web: 'Web') -> None:
        if not self.applied:
            self.applied = True
            for prefix, blueprint in self._enabled:
                self._apply_blueprint(web, prefix, symbol_by_name(blueprint))

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

    app: AppT

    driver_version: str

    views: MutableMapping[str, View]
    reverse_names: MutableMapping[str, str]

    blueprints: BlueprintManager

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        self.views = {}
        self.reverse_names = {}
        self.blueprints = BlueprintManager(self.default_blueprints)
        Service.__init__(self, **kwargs)

    @abc.abstractmethod
    def text(self, value: str, *, content_type: str = None,
             status: int = 200) -> Response:
        ...

    @abc.abstractmethod
    def html(self, value: str, *, status: int = 200) -> Response:
        ...

    @abc.abstractmethod
    def json(self, value: Any, *, status: int = 200) -> Response:
        ...

    @abc.abstractmethod
    def bytes(self,
              value: _bytes,
              *,
              content_type: str = None,
              status: int = 200) -> Response:
        ...

    @abc.abstractmethod
    def bytes_to_response(self, s: _bytes) -> Response:
        ...

    @abc.abstractmethod
    def response_to_bytes(self, response: Response) -> _bytes:
        ...

    @abc.abstractmethod
    def route(self, pattern: str, handler: Callable) -> None:
        ...

    @abc.abstractmethod
    def add_static(self,
                   prefix: str,
                   path: Union[Path, str],
                   **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    async def wsgi(self) -> Any:
        ...

    def add_view(self, view_cls: Type[View], *, prefix: str = '') -> View:
        view: View = view_cls(self.app, self)
        path = prefix.rstrip('/') + '/' + view.view_path.lstrip('/')
        self.route(path, view)
        self.views[path] = view
        self.reverse_names[view.view_name] = path
        return view

    def url_for(self, view_name: str, **kwargs: Any) -> str:
        """Get URL by view name

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
        self.blueprints.apply(self)
        self.app.on_webserver_init(self)

    @property
    def url(self) -> URL:
        canon = self.app.conf.canonical_url
        if canon.host == socket.gethostname():
            return URL(f'http://localhost:{self.app.conf.web_port}/')
        return self.app.conf.canonical_url


class Request:
    """HTTP Request."""

    method: str
    url: URL

    @property
    def match_info(self) -> MutableMapping[str, str]:
        ...

    @property
    def query(self) -> MutableMapping[str, str]:
        ...
