from typing import Any, Callable
from ..utils.services import Service

_bytes = bytes


class Response:
    ...


class Web(Service):
    app: Any

    def text(self, value: str) -> Any:
        ...

    def bytes(self, value: _bytes, *, content_type: str = None) -> Any:
        ...

    def route(self, pattern: str, handler: Callable) -> None:
        ...


class Request:
    method: str
