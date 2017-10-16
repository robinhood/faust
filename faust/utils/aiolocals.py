# -*- coding: utf-8 -*-
"""Local storage for coroutines.

Parts borrowed from werkzeug.local

:copyright: (c) 2013 by the Werkzeug Team, see AUTHORS for more details.
:license: BSD, see LICENSE for more details.
"""
import asyncio
import threading
from types import TracebackType
from typing import Any, List, MutableMapping, Type

__all__ = ['Local', 'Context']

R_CONTEXT = """
<{name} ident={self.ident} locals={self.locals!r} parent={self.parent!r}>
""".strip()


def _get_ident() -> int:
    """Return the current asyncio task id, or 0 if not found."""
    try:
        loop = asyncio.get_event_loop()
    # py3.4 raises AssertionError in this scenario; py3.5+ raises RuntimeError
    # See http://bugs.python.org/issue22926 for more info.
    except (AssertionError, RuntimeError):
        # We're in non-I/O thread at the moment.
        return threading.current_thread().task_ident  # type: ignore
    else:
        if loop.is_running():
            task = asyncio.Task.current_task(loop=loop)
            task_id = id(task)
            return task_id
        else:
            return 0


#: A map of task ids to Context objects.
_contexts: MutableMapping[int, 'Context'] = {}


class Context:
    """Task Context.

    Tracks a context, or set of locals for a given task.
    Should only be used as a context manager or via wrap_async.

    Keyword Arguments:
        ident: The function to use to find a local state identifier
        locals: A list of Local instances to manage
        parent: The parent context
    """

    def __init__(self,  # noqa: T002
                 *,
                 ident: int = None,
                 locals: List = None,
                 parent: 'Context' = None) -> None:
        self.ident = _get_ident() if ident is None else ident
        self.locals = [] if locals is None else locals

        if parent:
            self.parent = parent
            for l in parent.locals:
                l.__copy_from_parent__(self.ident, parent.ident)
        else:
            self.parent = None

    def cleanup(self) -> None:
        for local in self.locals:
            local.__release_local__(self.ident)

    def __enter__(self) -> 'Context':
        _contexts[self.ident] = self
        return self

    def __exit__(self,
                 exc_type: Type[BaseException] = None,
                 exc_val: BaseException = None,
                 exc_tb: TracebackType = None) -> None:
        self.cleanup()

    def __repr__(self) -> str:
        return R_CONTEXT.format(self=self, name=type(self).__name__)


def _spawn_context(child_ident: int, parent_ident: int = None) -> Context:
    parent = None if not parent_ident else _contexts.get(parent_ident)
    ctx = Context(
        ident=child_ident,
        locals=parent.locals if parent else [],
        parent=parent,
    )
    ctx.__enter__()
    return ctx


class Local:
    """Local storage for task.

    An object that stores different state for different task contexts.
    Meant to be used with Context.

    To use within a Context, simply set arbitrary attributes on this
    object and they will be tracked automatically.
    """

    __slots__ = ('__storage__', '__ident_func__')

    def __init__(self) -> None:
        object.__setattr__(self, '__storage__', {})
        object.__setattr__(self, '__ident_func__', _get_ident)

    def __release_local__(self, ident: int = None) -> None:
        if not ident:
            ident = self.__ident_func__()
        self.__storage__.pop(ident, None)

    def __copy_from_parent__(
            self, child_ident: int, parent_ident: int) -> None:
        self.__storage__[child_ident] = self.__storage__[parent_ident]

    def __getattr__(self, name: str) -> Any:
        try:
            ident = self.__ident_func__()
            if ident not in _contexts:
                raise ValueError('Cannot access a local outside a context')
            return self.__storage__[ident][name]
        except KeyError:
            raise AttributeError(name) from None

    def __setattr__(self, name: str, value: Any) -> None:
        ident = self.__ident_func__()
        if ident not in _contexts:
            raise ValueError('Cannot set a local outside a context')
        storage = self.__storage__
        try:
            storage[ident][name] = value
        except KeyError:
            storage[ident] = {name: value}

    def __delattr__(self, name: str) -> None:
        try:
            ident = self.__ident_func__()
            if ident not in _contexts:
                raise ValueError('Cannot set a local outside a context')
            del self.__storage__[ident][name]
        except KeyError:
            raise AttributeError(name) from None
