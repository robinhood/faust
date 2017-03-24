import asyncio
from typing import Sequence
from .utils.services import Service
from .types import ServiceT


class Worker(Service):

    services: Sequence[ServiceT]

    def __init__(self, *services: ServiceT,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.services = services
        super().__init__(loop=loop)

    async def on_start(self) -> None:
        for service in self.services:
            await service.start()

    async def on_stop(self) -> None:
        for service in reversed(self.services):
            await service.stop()
