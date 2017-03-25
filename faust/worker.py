import asyncio
from typing import IO, Sequence, Set, Union, cast
from .utils.logging import setup_logging
from .utils.services import Service
from .types import AppT, ServiceT, SensorT


class Worker(Service):

    sensors: Set[SensorT]
    services: Sequence[ServiceT]

    def __init__(self, *services: ServiceT,
                 sensors: Sequence[SensorT] = None,
                 loglevel: Union[str, int] = None,
                 logfile: Union[str, IO] = None,
                 logformat: str = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.services = services
        self.sensors = set(sensors or [])
        self.loglevel = loglevel
        self.logfile = logfile
        self.logformat = logformat
        super().__init__(loop=loop)

    async def on_start(self) -> None:
        if self.loglevel:
            setup_logging(
                loglevel=self.loglevel,
                logfile=self.logfile,
                logformat=self.logformat,
            )
        for sensor in self.sensors:
            await sensor.maybe_start()
        for service in self.services:
            for sensor in self.sensors:
                if isinstance(service, AppT):
                    cast(AppT, service).add_sensor(sensor)
            await service.maybe_start()

    async def on_stop(self) -> None:
        for service in reversed(self.services):
            await service.stop()
        for sensor in self.sensors:
            await sensor.stop()
