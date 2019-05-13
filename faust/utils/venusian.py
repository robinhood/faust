"""Venusian (see :pypi:`venusian`).

We define our own interface so we don't have to specify the
callback argument.
"""
import sys
import venusian
from typing import Any, Callable
from venusian import Scanner, attach as _attach
from .logging import get_logger

__all__ = ['Scanner', 'attach']

logger = get_logger(__name__)


def attach(fun: Callable, category: str, *,
           callback: Callable[[Scanner, str, Any], None] = None,
           **kwargs: Any) -> None:
    """Shortcut for :func:`venusian.attach`.

    This shortcut makes the callback argument optional.
    """
    callback = _on_found if callback is None else callback
    return _attach(fun, callback, category=category,
                   on_error=_log_venusian_error, **kwargs)


def _on_found(scanner: venusian.Scanner, name: str, obj: Any) -> None:
    ...


def _log_venusian_error(name: str) -> None:
    logger.warning('Autodiscovery importing module %r raised error: %r',
                   name, sys.exc_info()[1], exc_info=1)
