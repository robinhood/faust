import asyncio
from typing import Callable, List


class Service:

    shutdown_timeout = 60.0

    _started: asyncio.Event
    _stopped: asyncio.Event
    _shutdown: asyncio.Event
    _polling_started: bool
    _pollers: List

    def __init__(self, *, loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop or asyncio.get_event_loop()
        self._started = asyncio.Event()
        self._stopped = asyncio.Event()
        self._shutdown = asyncio.Event()
        self._polling_started = False
        self._pollers = []
        self.on_init()

    def on_init(self) -> None:
        ...

    async def on_start(self) -> None:
        ...

    async def on_stop(self) -> None:
        ...

    async def on_shutdown(self) -> None:
        ...

    async def start(self) -> None:
        await self.on_start()
        self._started.set()

    async def stop(self) -> None:
        self._stopped.set()
        await self.on_stop()
        if self._polling_started:
            await asyncio.wait_for(self._shutdown.wait(),  # type: ignore
                                   timeout=self.shutdown_timeout)
        await self.on_shutdown()

    def __repr__(self) -> str:
        return '<{name}: {self.state}>'.format(
            name=type(self).__name__, self=self)

    def add_poller(self, callback: Callable) -> None:
        if not self._polling_started:
            self._polling_started = True
            self._restart_polling_callbacks()
        self._pollers.append(callback)

    async def _call_polling_callbacks(self) -> None:
        if self._stopped.is_set():
            self._shutdown.set()
        else:
            for poller in self._pollers:
                await poller()
            # we add this to the loop so this call is not recursive.
            self.loop.call_soon(self._restart_polling_callbacks)

    def _restart_polling_callbacks(self) -> None:
        asyncio.ensure_future(self._call_polling_callbacks())

    @property
    def state(self) -> str:
        if not self._started.is_set():
            return 'init'
        if not self._stopped.is_set():
            return 'running'
        if not self._shutdown.is_set():
            return 'stopping'
        return 'shutdown'
