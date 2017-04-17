from typing import Any, Callable
from ..utils.services import Service


class Web(Service):
    app: Any

    def text(self, value: str) -> Any:
        ...

    def route(self, pattern: str, handler: Callable) -> None:
        ...


class Request:
    method: str
