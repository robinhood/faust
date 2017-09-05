import math
import signal
import traceback
from types import FrameType
from typing import Any, Type
from .logging import get_logger
from .services import Service
from .times import Seconds, want_seconds

__all__ = ['Blocking', 'BlockingDetector']

if hasattr(signal, 'setitimer'):
    def arm_alarm(seconds: float) -> None:
        signal.setitimer(signal.ITIMER_REAL, seconds)
else:
    try:
        import itimer
    except ImportError:
        def arm_alarm(seconds: float) -> None:
            signal.alarm(math.ceil(seconds))
    else:
        def arm_alarm(seconds: float) -> None:
            return itimer(seconds)


logger = get_logger(__name__)


class Blocking(RuntimeError):
    ...


class BlockingDetector(Service):
    logger = logger

    def __init__(self,
                 timeout: Seconds,
                 raises: Type[Exception] = Blocking,
                 **kwargs: Any) -> None:
        self.timeout: float = want_seconds(timeout)
        self.raises: Type[Exception] = raises
        super().__init__(**kwargs)

    @Service.task
    async def _deadman_switch(self) -> None:
        try:
            while 1:
                self._reset_signal()
                await self.sleep(self.timeout * 0.96)
        finally:
            self._clear_signal()

    def _reset_signal(self) -> None:
        signal.signal(signal.SIGALRM, self._on_alarm)
        self._arm(self.timeout)

    def _clear_signal(self) -> None:
        self._arm(0)

    def _arm(self, timeout: float) -> None:
        arm_alarm(timeout)

    def _on_alarm(self, signum: int, frame: FrameType) -> None:
        msg = f'Blocking detected (timeout={self.timeout})'
        stack = ''.join(traceback.format_stack(frame))
        self.log.warn(f'{msg}: {stack}')
        self._reset_signal()
        raise self.raises(msg)
