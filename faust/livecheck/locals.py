"""Locals - Current test & execution context."""
import typing
from typing import Optional
from mode.locals import LocalStack
from .models import TestExecution

if typing.TYPE_CHECKING:
    from .runners import TestRunner as _TestRunner
else:  # pragma: no cover
    class _TestRunner: ...   # noqa

__all__ = [
    'current_execution',
    'current_execution_stack',
    'current_test',
    'current_test_stack',
]

current_test_stack: LocalStack[TestExecution]
current_test_stack = LocalStack()

current_execution_stack: LocalStack[_TestRunner]
current_execution_stack = LocalStack()


def current_execution() -> Optional[_TestRunner]:
    """Return the current :class:`~faust.livecheck.TestRunner`."""
    return current_execution_stack.top


def current_test() -> Optional[TestExecution]:
    """Return information about the current test (if any)."""
    return current_test_stack.top
