"""Logging utilities."""
import logging
from typing import Any, IO, Union

__all__ = ['get_logger']

DEFAULT_FORMAT = '[%(asctime)s: %(levelname)s] %(message)s'


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def setup_logging(
        *,
        loglevel: Union[str, int] = None,
        logfile: Union[str, IO] = None,
        logformat: str = None) -> int:
    stream: IO = None
    _loglevel: int
    if isinstance(loglevel, str):
        _loglevel = logging.getLevelName(loglevel.upper())  # type: ignore
    else:
        _loglevel = loglevel
    if not isinstance(logfile, str):
        stream, logfile = logfile, None
    _setup_logging(
        level=_loglevel,
        filename=logfile,
        stream=stream,
        format=logformat or DEFAULT_FORMAT,
    )
    logging.root.handlers[0].setLevel(_loglevel)
    return _loglevel


def _setup_logging(**kwargs: Any) -> None:
    # stupid logging just have to crash if both stream/loglevel
    # set EVEN IF ONE OF THEM IS SET TO NONE AAAAAAAAAAAAAAAAAAAAAHG
    if 'stream' in kwargs:
        del kwargs['filename']
    logging.basicConfig(**kwargs)
