from typing import Optional
from mode.locals import LocalStack
from .models import TestExecution

__all__ = ['current_test', 'current_test_stack']

current_test_stack: LocalStack[TestExecution]
current_test_stack = LocalStack()


def current_test() -> Optional[TestExecution]:
    """Return information about the current test (if any)."""
    return current_test_stack.top
