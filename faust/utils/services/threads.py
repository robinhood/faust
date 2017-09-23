import asyncio
import os
import sys
import threading
import traceback
from typing import Any
from .types import ServiceT

__all__ = ['ServiceThread']


class ServiceThread(threading.Thread):
    _shutdown: threading.Event
    _stopped: threading.Event

    def __init__(self, service: ServiceT,
                 *,
                 daemon: bool = False,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.service = service
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self._loop = loop
        super().__init__(daemon=daemon)

    def run(self) -> None:
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._serve(self._loop))

    async def _start_service(self, loop: asyncio.AbstractEventLoop) -> None:
        service = self.service
        service.loop = loop
        await service.start()
        await self.on_start()

    async def on_start(self) -> None:
        ...

    async def on_restart(self) -> None:
        ...

    async def _stop_service(self) -> None:
        await self.service.stop()
        await self.on_stop()

    async def on_stop(self) -> None:
        ...

    async def _serve(self, loop: asyncio.AbstractEventLoop) -> None:
        shutdown_set = self._shutdown.is_set
        await self._start_service(loop)
        try:
            while not shutdown_set():
                try:
                    await asyncio.sleep(1, loop=loop)
                except BaseException as exc:  # pylint: disable=broad-except
                    try:
                        self.on_crash('{0!r} crashed: {1!r}', self.name, exc)
                        self._set_stopped()
                    finally:
                        os._exit(1)  # exiting by normal means won't work
        finally:
            try:
                await self._stop_service()
            finally:
                self._set_stopped()

    def on_crash(self, msg: str, *fmt: Any, **kwargs: Any) -> None:
        print(msg.format(*fmt), file=sys.stderr)
        traceback.print_exc(None, sys.stderr)

    def _set_stopped(self) -> None:
        try:
            self._stopped.set()
        except TypeError:
            # we lost the race at interpreter shutdown,
            # so gc collected built-in modules.
            pass

    def stop(self) -> None:
        """Graceful shutdown."""
        self._shutdown.set()
        self._stopped.wait()
        if self.is_alive():
            self.join(threading.TIMEOUT_MAX)
