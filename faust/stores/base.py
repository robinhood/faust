from ..types import AppT, StoreT


class Store(StoreT):

    def __init__(self, url: str, app: AppT) -> None:
        self.url = url
        self.app = app
        self.on_init()

    def on_init(self) -> None:
        ...
