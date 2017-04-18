from typing import Any
from ..types import AppT, StoreT
from ..utils.services import Service


class Store(StoreT, Service):

    def __init__(self, url: str, app: AppT, **kwargs: Any) -> None:
        self.url = url
        self.app = app
        super().__init__(**kwargs)
