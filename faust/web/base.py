from typing import Any, Callable
from ..types import AppT
from ..utils.logging import get_logger
from ..utils.services import Service

__all__ = ['Request', 'Response', 'Web']

logger = get_logger(__name__)

_bytes = bytes


class Response:
    ...


class Web(Service):
    logger = logger
    app: AppT

    bind: str
    port: int

    driver_version: str

    def __init__(self, app: AppT,
                 *,
                 port: int = None,
                 bind: str = None,
                 **kwargs: Any) -> None:
        ...

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
        return f'http://localhost:{self.port}/'


class Request:
    method: str
