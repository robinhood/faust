import asyncio
from typing import Any, Awaitable, Callable, Dict, List, Type, cast
from .exceptions import MaxRestartsExceeded
from .services import Service
from .types import ServiceT, SupervisorStrategyT
from ..futures import notify
from ..logging import get_logger
from ..times import Bucket, Seconds, rate_limit, want_seconds

__all__ = [
    'SupervisorStrategy',
    'OneForOneSupervisor',
    'OneForAllSupervisor',
]

logger = get_logger(__name__)


class SupervisorStrategy(Service, SupervisorStrategyT):
    _please_wakeup: asyncio.Future
    _services: List[ServiceT]
    _bucket: Bucket
    _index: Dict[ServiceT, int]

    def __init__(self,
                 *services: ServiceT,
                 max_restarts: Seconds = 100.0,
                 over: Seconds = 1.0,
                 raises: Type[BaseException] = MaxRestartsExceeded,
                 replacement: Callable[[ServiceT, int],
                                       Awaitable[ServiceT]] = None,
                 **kwargs: Any) -> None:
        self.max_restarts = want_seconds(max_restarts)
        self.over = want_seconds(over)
        self.raises = raises
        self._bucket = rate_limit(self.max_restarts, self.over, raises=raises)
        self._services = list(services or [])
        self.replacement = replacement
        self._please_wakeup = None
        self._index = {}
        super().__init__(**kwargs)

    def wakeup(self) -> None:
        notify(self._please_wakeup)

    def add(self, service: ServiceT) -> None:
        # XXX not thread-safe, but shouldn't have to be.
        size = len(self._services)
        self._services.append(service)
        self._index[service] = size + 1 if size else size
        assert service.supervisor is None
        # Setting the service.supervisor here means that crashing
        # the service will delegate to the supervisor, and not cause
        # the service tree to be traversed and crashed.
        service.supervisor = self

    def discard(self, service: ServiceT) -> None:
        self._index.pop(service, None)
        try:
            self._services.remove(service)
        except ValueError:
            pass

    def insert(self, index: int, service: ServiceT) -> None:
        old_service, self._services[index] = self._services[index], service
        service.supervisor = self
        del self._index[old_service]
        self._index[service] = index

    def service_operational(self, service: ServiceT) -> bool:
        return not service.crashed

    @Service.task
    async def _supervisor(self) -> None:
        services = self._services

        while not self.should_stop:
            self._please_wakeup = asyncio.Future(loop=self.loop)
            try:
                await asyncio.wait_for(self._please_wakeup, timeout=5.0)
            except asyncio.TimeoutError:
                pass
            finally:
                self._please_wakeup = None

            to_start: List[ServiceT] = []
            to_restart: List[ServiceT] = []
            for service in services:
                if service.started:
                    if not self.service_operational(service):
                        to_restart.append(service)
                else:
                    to_start.append(service)

            await self.start_services(to_start)
            await self.restart_services(to_restart)

    async def on_stop(self) -> None:
        for service in self._services:
            if service.started:
                try:
                    await service.stop()
                except Exception as exc:
                    self.log.exception(
                        'Cannot stop service %r: %r', service, exc)

    async def start_services(self, services: List[ServiceT]) -> None:
        for service in services:
            await self.start_service(service)

    async def start_service(self, service: ServiceT) -> None:
        await service.start()

    async def restart_services(self, services: List[ServiceT]) -> None:
        for service in services:
            await self.restart_service(service)

    async def restart_service(self, service: ServiceT) -> None:
        self.log.info('Restarting dead %r! Last crash reason: %r',
                      service, cast(Service, service)._crash_reason)
        async with self._bucket:
            if self.replacement:
                index = self._index[service]
                new_service = await self.replacement(service, index)
                new_service.supervisor = self
                self.insert(index, new_service)
            else:
                await service.restart()


class OneForOneSupervisor(SupervisorStrategy):
    ...


class OneForAllSupervisor(SupervisorStrategy):

    async def restart_services(self, services: List[ServiceT]) -> None:
        # we ignore the list of actual crashed services,
        # and restart all of them
        if services:
            # Stop them all simultaneously.
            await asyncio.wait(
                [service.stop() for service in self._services],
                return_when=asyncio.ALL_COMPLETED,
                loop=self.loop,
            )
            # Then restart them one by one.
            for service in self._services:
                await self.restart_service(service)
