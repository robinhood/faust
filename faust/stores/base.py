import asyncio
from ..types import AppT, StoreT


class Store(StoreT):

    def __init__(self, url: str, app: AppT,
                 *,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.url = url
        self.app = app
        self.loop = loop or asyncio.get_event_loop()
        self.on_init()

    def on_init(self) -> None:
        ...
