"""OpenTracing utilities."""
import asyncio
import sys
import typing
from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable, Optional, Tuple
import opentracing
from mode import shortlabel

__all__ = [
    'current_span',
    'noop_span',
    'set_current_span',
    'finish_span',
    'operation_name_from_fun',
    'traced_from_parent_span',
    'call_with_trace',
]

if typing.TYPE_CHECKING:
    _current_span: ContextVar[opentracing.Span]
_current_span = ContextVar('current_span')


def current_span() -> Optional[opentracing.Span]:
    """Get the current span for this context (if any)."""
    return _current_span.get(None)


def set_current_span(span: opentracing.Span) -> None:
    """Set the current span for the current context."""
    _current_span.set(span)


def noop_span() -> opentracing.Span:
    """Return a span that does nothing when traced."""
    return opentracing.Tracer()._noop_span


def finish_span(span: Optional[opentracing.Span], *,
                error: BaseException = None) -> None:
    """Finish span, and optionally set error tag."""
    if span is not None:
        if error:
            span.__exit__(type(error), error, error.__traceback__)
        else:
            span.finish()


def operation_name_from_fun(fun: Any) -> str:
    """Generate opentracing name from function."""
    obj = getattr(fun, '__self__', None)
    if obj is not None:
        objlabel = shortlabel(obj)
        funlabel = shortlabel(fun)
        if funlabel.startswith(objlabel):
            # remove obj name from function label
            funlabel = funlabel[len(objlabel):]
        return f'{objlabel}-{funlabel}'
    else:
        return f'{shortlabel(fun)}'


def traced_from_parent_span(parent_span: opentracing.Span = None,
                            **extra_context: Any) -> Callable:
    """Decorate fucntion to be traced from parent span."""
    def _wrapper(fun: Callable, **more_context: Any) -> Callable:
        operation_name = operation_name_from_fun(fun)
        @wraps(fun)
        def _inner(*args: Any, **kwargs: Any) -> Any:
            parent = parent_span
            if parent is None:
                parent = current_span()
            if parent is not None:
                child = parent.tracer.start_span(
                    operation_name=operation_name,
                    child_of=parent,
                    tags={**extra_context, **more_context},
                )
                callback = (_restore_span, (parent, child))
                set_current_span(child)
                return call_with_trace(
                    child, fun, callback, *args, **kwargs)
            return fun(*args, **kwargs)
        return _inner
    return _wrapper


def _restore_span(span: opentracing.Span,
                  expected_current_span: opentracing.Span) -> None:
    current = current_span()
    assert current is expected_current_span
    set_current_span(span)


def call_with_trace(span: opentracing.Span,
                    fun: Callable,
                    callback: Optional[Tuple[Callable, Tuple[Any, ...]]],
                    *args: Any, **kwargs: Any) -> Any:
    """Call function and trace it from parent span."""
    cb: Optional[Callable] = None
    cb_args: Tuple = ()
    if callback:
        cb, cb_args = callback
    span.__enter__()
    try:
        ret = fun(*args, **kwargs)
    except BaseException:
        span.__exit__(*sys.exc_info())
        raise
    else:
        if asyncio.iscoroutine(ret):
            # if async def method, we attach our span to
            # when it completes.
            async def corowrapped() -> Any:
                try:
                    await ret
                except BaseException:
                    span.__exit__(*sys.exc_info())
                    if cb:
                        cb(*cb_args)
                    raise
                else:
                    span.__exit__(None, None, None)
                    if cb:
                        cb(*cb_args)
            return corowrapped()
        else:
            # for non async def method, we just exit the span.
            span.__exit__(None, None, None)
            if cb:
                cb(*cb_args)
            return ret
