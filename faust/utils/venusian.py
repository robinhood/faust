"""Venusian (see :pypi:`venusian`).

We define our own interface so we don't have to specify the
callback argument.
"""
import venusian
from typing import Any, Callable
from venusian import Scanner, attach as _attach

__all__ = ['Scanner', 'attach']


def attach(fun: Callable, category: str, *,
           callback: Callable[[Scanner, str, Any], None] = None,
           **kwargs: Any) -> None:
    """Shortcut for :func:`venusian.attach`.

    This shortcut makes the callback argument optional.
    """
    callback = _on_found if callback is None else callback
    return _attach(fun, callback, category=category, **kwargs)


def _on_found(scanner: venusian.Scanner, name: str, obj: Any) -> None:
    ...
