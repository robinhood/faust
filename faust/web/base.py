"""Base interface for Web server and views."""
from pathlib import Path
from typing import Any, Callable, MutableMapping, Union
from mode import Service
from yarl import URL
from faust.cli._env import WEB_BIND, WEB_PORT
from faust.types import AppT

__all__ = ['Request', 'Response', 'Web']

_bytes = bytes


class Response:
    """Web server response and status."""


class Web(Service):
    """Web server and HTTP interface."""

    app: AppT

    bind: str
    port: int

    driver_version: str

    def __init__(self,
                 app: AppT,
                 *,
                 port: int = None,
                 bind: str = None,
                 **kwargs: Any) -> None:
        self.app = app
        self.port = port or WEB_PORT
        self.bind = bind or WEB_BIND
        super().__init__(**kwargs)

    def text(self, value: str, *, content_type: str = None,
             status: int = 200) -> Response:
        ...

    def html(self, value: str, *, status: int = 200) -> Response:
        ...

    def json(self, value: Any, *, status: int = 200) -> Response:
        ...

    def bytes(self,
              value: _bytes,
              *,
              content_type: str = None,
              status: int = 200) -> Response:
        ...

    def route(self, pattern: str, handler: Callable) -> None:
        ...

    def add_static(self,
                   prefix: str,
                   path: Union[Path, str],
                   **kwargs: Any) -> None:
        ...

    @property
    def url(self) -> URL:
        return URL(f'http://localhost:{self.port}/')


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
