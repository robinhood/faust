import asyncio
from typing import Any, IO, Sequence, Set, Union, cast
from .utils.compat import DummyContext
from .utils.logging import setup_logging
from .utils.services import Service
from .utils.imports import SymbolArg, symbol_by_name
from .types import AppT, ServiceT, SensorT


class Worker(Service):
    debug: False
    sensors: Set[SensorT]
    services: Sequence[ServiceT]
    loglevel: Union[str, int]
    logfile: Union[str, IO]

    def __init__(self, *services: ServiceT,
                 sensors: Sequence[SensorT] = None,
                 debug: bool = False,
                 loglevel: Union[str, int] = None,
                 logfile: Union[str, IO] = None,
                 logformat: str = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.services = services
        self.sensors = set(sensors or [])
        self.debug = debug
        self.loglevel = loglevel
        self.logfile = logfile
        self.logformat = logformat
        super().__init__(loop=loop)

    def execute_from_commandline(self, *coroutines):
        with self._monitor():
            asyncio.gather(
                *[asyncio.ensure_future(coro, loop=self.loop)
                for coro in coroutines],
                loop=self.loop)
            self.loop.run_until_complete(self.start())
            try:
                self.loop.run_forever()
            except KeyboardInterrupt:
                pass
            finally:
                self.loop.close()

    def _monitor(self) -> Any:
        if self.debug:
            try:
                import aiomonitor
            except ImportError:
                pass
            else:
                return aiomonitor.start_monitor(loop=self.loop)
        return DummyContext()

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
