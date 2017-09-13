"""Faust environment variables."""
import os

__all__ = ['DEBUG', 'DEFAULT_BLOCKING_TIMEOUT', 'WEB_PORT', 'WEB_BIND']

#: Enables debugging features (like blockdetection).
DEBUG: bool = bool(os.environ.get('F_DEBUG', False))

#: Blocking detection timeout
DEFAULT_BLOCKING_TIMEOUT: float = float(os.environ.get(
    'F_BLOCKING_TIMEOUT', '10.0',
))

WEB_PORT: int = int(os.environ.get('F_WEB_PORT', '6066'))
WEB_BIND: str = os.environ.get('F_WEB_BIND', '0.0.0.0')
