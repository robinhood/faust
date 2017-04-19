"""Logging utilities."""
import logging
from typing import Any, IO, Union

__all__ = ['get_logger', 'level_name', 'level_number', 'setup_logging']

DEFAULT_FORMAT = '[%(asctime)s: %(levelname)s] %(message)s'


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def level_name(loglevel: Union[str, int]) -> str:
    if isinstance(loglevel, str):
        return loglevel.upper()
    return logging.getLevelName(loglevel)


def level_number(loglevel: Union[str, int]) -> int:
    if isinstance(loglevel, int):
        return loglevel
    return logging.getLevelName(loglevel.upper())  # type: ignore


def setup_logging(
        *,
        loglevel: Union[str, int] = None,
        logfile: Union[str, IO] = None,
        logformat: str = None) -> int:
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


def _setup_logging(**kwargs: Any) -> None:
    # stupid logging just have to crash if both stream/loglevel
    # set EVEN IF ONE OF THEM IS SET TO NONE AAAAAAAAAAAAAAAAAAAAAHG
    if 'stream' in kwargs:
        del kwargs['filename']
    logging.basicConfig(**kwargs)
