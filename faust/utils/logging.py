"""Logging utilities."""
import logging
import sys
import threading
import traceback
from functools import singledispatch
from pprint import pprint
from typing import Any, IO, Union

__all__ = [
    'CompositeLogger',
    'cry', 'get_logger', 'level_name', 'level_number', 'setup_logging',
]

DEFAULT_FORMAT = '[%(asctime)s: %(levelname)s] %(message)s'


def get_logger(name: str) -> logging.Logger:
    """Get logger by name."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


@singledispatch
def level_name(loglevel: int) -> str:
    """Convert log level to number."""
    return logging.getLevelName(loglevel)


@level_name.register(str)
def _when_str(loglevel: str) -> str:
    return loglevel.upper()


@singledispatch
def level_number(loglevel: int) -> int:
    """Convert log level number to name."""
    return loglevel


@level_number.register(str)
def _(loglevel: str) -> int:
    return logging.getLevelName(loglevel.upper())  # type: ignore


def setup_logging(
        *,
        loglevel: Union[str, int] = None,
        logfile: Union[str, IO] = None,
        logformat: str = None) -> int:
    """Setup logging to file/stream."""
    stream: IO = None
    _loglevel: int = level_number(loglevel)
    if not isinstance(logfile, str):
        stream, logfile = logfile, None
    _setup_logging(
        level=_loglevel,
        filename=logfile,
        stream=stream,
        format=logformat or DEFAULT_FORMAT,
    )
    return _loglevel


class CompositeLogger:
    logger: logging.Logger

    def __init__(self, obj: Any) -> None:
        self._obj: Any = obj

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.log(logging.INFO, msg, *args, **kwargs)

    def warn(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.log(logging.WARN, msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.log(logging.ERROR, msg, *args, **kwargs)

    def crit(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.log(logging.CRITICAL, msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.log(logging.ERROR, msg, *args, exc_info=1, **kwargs)

    def log(self, severity: int, msg: str, *args: Any, **kwargs: Any) -> None:
        self._obj._log(
            severity, self.format(severity, msg, *args, **kwargs),
            *args, **kwargs)

    def format(self, severity: int, msg: str,
               *args: Any, **kwargs: Any) -> str:
        return self._obj._format_log(severity, msg, *args, **kwargs)


def _setup_logging(**kwargs: Any) -> None:
    # stupid logging just have to crash if both stream/loglevel
    # set EVEN IF ONE OF THEM IS SET TO NONE AAAAAAAAAAAAAAAAAAAAAHG
    if 'stream' in kwargs:
        del kwargs['filename']
    logging.basicConfig(**kwargs)


def cry(file: IO, sepchr: str = '=', seplen: int =49) -> None:
    """Return stack-trace of all active threads.

    See Also:
        Taken from https://gist.github.com/737056.
    """
    # get a map of threads by their ID so we can print their names
    # during the traceback dump
    tmap = {t.ident: t for t in threading.enumerate()}

    sep = sepchr * seplen
    for tid, frame in sys._current_frames().items():
        thread = tmap.get(tid)
        if thread:
            print(f'{thread.name}', file=file)            # noqa: T003
            print(sep, file=file)                         # noqa: T003
            traceback.print_stack(frame, file=file)
            print(sep, file=file)                         # noqa: T003
            print('LOCAL VARIABLES', file=file)           # noqa: T003
            print(sep, file=file)                         # noqa: T003
            pprint(frame.f_locals, stream=file)
            print('\n', file=file)                        # noqa: T003
