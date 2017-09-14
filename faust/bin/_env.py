"""Faust environment variables."""
import os

__all__ = [
    'BLOCKING_TIMEOUT',
    'DATADIR',
    'DEBUG',
    'WEB_PORT',
    'WEB_BIND',
    'WORKDIR',
]

#: Enables debugging features (like blockdetection).
DEBUG: bool = bool(os.environ.get('F_DEBUG', False))

#: Working directory to change into at start.
WORKDIR: str = os.environ.get('F_WORKDIR')

#: Directory to keep the application state (tables, checkpoint, etc).
DATADIR: str = os.environ.get('F_DATADIR', '{appid}-data')

#: Blocking detection timeout
BLOCKING_TIMEOUT: float = float(os.environ.get('F_BLOCKING_TIMEOUT', '10.0'))

WEB_PORT: int = int(os.environ.get('F_WEB_PORT', '6066'))
WEB_BIND: str = os.environ.get('F_WEB_BIND', '0.0.0.0')
