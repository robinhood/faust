from typing import Any, Callable
from ..utils.services import Service

__all__ = ['Request', 'Response', 'Web']

_bytes = bytes


class Response:
    ...


class Web(Service):
    app: Any

    bind: str
    port: int

    driver_version: str

    def text(self, value: str) -> Any:
        ...

    def html(self, value: str) -> Any:
        ...

    def json(self, value: Any) -> Any:
        ...

    def bytes(self, value: _bytes, *, content_type: str = None) -> Any:
        ...

    def route(self, pattern: str, handler: Callable) -> None:
        ...

    @property
    def url(self) -> str:
        return 'http://localhost:{}/'.format(self.port)


class Request:
    method: str
