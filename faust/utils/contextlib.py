try:
    from contextlib import AbstractContextManager, asynccontextmanager
except ImportError:
    import abc
    import sys
    from typing import Any, Callable, Mapping, Sequence, Type
    from functools import wraps

    class AbstractContextManager(abc.ABC):
        """An abstract base class for context managers."""

        def __enter__(self) -> 'AbstractContextManager':
            """Return `self` upon entering the runtime context."""
            return self

        @abc.abstractmethod
        def __exit__(
                self,
                exc_type: Type,
                exc_value: Exception,
                traceback: Any) -> Any:
            """Raise any exception triggered within the runtime context."""
            return None

        @classmethod
        def __subclasshook__(cls, C: Type) -> bool:
            if cls is AbstractContextManager:
                if (any('__enter__' in B.__dict__ for B in C.__mro__) and
                        any('__exit__' in B.__dict__ for B in C.__mro__)):
                    return True
            return NotImplemented

    class ContextDecorator(object):

        def _recreate_cm(self) -> 'ContextDecorator':
            """Return a recreated instance of self.
            Allows an otherwise one-shot context manager like
            _GeneratorContextManager to support use as
            a decorator via implicit recreation.
            This is a private interface just for _GeneratorContextManager.
            See issue #11647 for details.
            """
            return self

        def __call__(self, fun: Callable) -> Callable:
            @wraps(fun)
            def inner(*args: Any, **kwds: Any):
                with self._recreate_cm():
                    return fun(*args, **kwds)
            return inner

    class _GeneratorContextManagerBase:
        """Shared functionality.

        for @contextmanager and @asynccontextmanager.
        """

        def __init__(self, fun: Callable, args: Sequence, kwds: Mapping):
            self.gen = fun(*args, **kwds)
            self.func, self.args, self.kwds = fun, args, kwds
            # Issue 19330: ensure context manager instances have
            # good docstrings
            doc = getattr(fun, '__doc__', None)
            if doc is None:
                doc = type(self).__doc__
            self.__doc__ = doc
            # Unfortunately, this still doesn't provide good help output when
            # inspecting the created context manager instances, since pydoc
            # currently bypasses the instance docstring and shows the docstring
            # for the class instead.
            # See http://bugs.python.org/issue19404 for more details.

    class _GeneratorContextManager(_GeneratorContextManagerBase,
                                   AbstractContextManager,
                                   ContextDecorator):
        """Helper for @contextmanager decorator."""

        def _recreate_cm(self) -> '_GeneratorContextManager':
            # _GCM instances are one-shot context managers, so the
            # CM must be recreated each time a decorated function is
            # called
            return self.__class__(self.func, self.args, self.kwds)

        def __enter__(self) -> Any:
            try:
                return next(self.gen)
            except StopIteration:
                raise RuntimeError("generator didn't yield") from None

        def __exit__(
                self, type: Type, value: Exception, traceback: Any) -> Any:
            if type is None:
                try:
                    next(self.gen)
                except StopIteration:
                    return False
                else:
                    raise RuntimeError("generator didn't stop")
            else:
                if value is None:
                    # Need to force instantiation so we can reliably
                    # tell if we get the same exception back
                    value = type()
                try:
                    self.gen.throw(type, value, traceback)
                except StopIteration as exc:
                    # Suppress StopIteration *unless* it's the same exception
                    # that was passed to throw().  This prevents
                    # a StopIteration # raised inside the "with" statement
                    # from being suppressed.
                    return exc is not value
                except RuntimeError as exc:
                    # Don't re-raise the passed in exception. (issue27122)
                    if exc is value:
                        return False
                    # Likewise, avoid suppressing if a StopIteration exception
                    # was passed to throw() and later wrapped
                    # into a RuntimeError (see PEP 479).
                    if type is StopIteration and exc.__cause__ is value:
                        return False
                    raise
                except:  # noqa: B901
                    # only re-raise if it's *not* the exception that was
                    # passed to throw(), because __exit__() must not raise
                    # an exception unless __exit__() itself failed.
                    # But throw() has to raise the exception to signal
                    # propagation, so this fixes the impedance mismatch
                    # between the throw() protocol and the __exit__() protocol.
                    #
                    # This cannot use 'except BaseException as exc' (as in the
                    # async implementation) to maintain compatibility with
                    # Python 2, where old-style class exceptions are not caught
                    # by 'except BaseException'.
                    if sys.exc_info()[1] is value:
                        return False
                    raise
                raise RuntimeError("generator didn't stop after throw()")

    class _AsyncGeneratorContextManager(_GeneratorContextManagerBase):
        """Helper for @asynccontextmanager."""

        async def __aenter__(self) -> Any:
            try:
                return await self.gen.__anext__()
            except StopAsyncIteration:
                raise RuntimeError("generator didn't yield") from None

        async def __aexit__(
                self, typ: Type, value: Exception, traceback: Any) -> Any:
            if typ is None:
                try:
                    await self.gen.__anext__()
                except StopAsyncIteration:
                    return
                else:
                    raise RuntimeError("generator didn't stop")
            else:
                if value is None:
                    value = typ()
                # See _GeneratorContextManager.__exit__ for comments on
                # subtleties in this implementation
                try:
                    await self.gen.athrow(typ, value, traceback)
                    raise RuntimeError("generator didn't stop after throw()")
                except StopAsyncIteration as exc:
                    return exc is not value
                except RuntimeError as exc:
                    if exc is value:
                        return False
                    # Avoid suppressing if a StopIteration exception
                    # was passed to throw() and later wrapped into
                    # a RuntimeError (see PEP 479 for sync generators;
                    # async generators also have this behavior). But do
                    # this only if the exception wrapped by the
                    # RuntimeError is actully Stop(Async)Iteration (see
                    # issue29692).
                    if isinstance(value, (StopIteration, StopAsyncIteration)):
                        if exc.__cause__ is value:
                            return False
                    raise
                except BaseException as exc:
                    if exc is not value:
                        raise

    def asynccontextmanager(func) -> Any:
        """@asynccontextmanager decorator.
        Typical usage:
            @asynccontextmanager
            async def some_async_generator(<arguments>):
                <setup>
                try:
                    yield <value>
                finally:
                    <cleanup>
        This makes this:
            async with some_async_generator(<arguments>) as <variable>:
                <body>
        equivalent to this:
            <setup>
            try:
                <variable> = <value>
                <body>
            finally:
                <cleanup>
        """
        @wraps(func)
        def helper(*args: Any, **kwds: Any):
            return _AsyncGeneratorContextManager(func, args, kwds)
        return helper
