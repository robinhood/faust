import asyncio
import logging
import sys
import typing
import traceback
from functools import partial
from typing import Any, Iterable, Optional, Type

from faust.exceptions import ImproperlyConfigured
from faust.types import AppT

try:
    import raven
except ImportError:  # pragma: no cover
    raven = None        # noqa

try:
    import sentry_sdk
except ImportError:  # pragma: no cover
    sentry_sdk = None       # noqa
    _sdk_aiohttp = None     # noqa
else:
    import sentry_sdk.integrations.aiohttp as _sdk_aiohttp  # type: ignore

try:
    import raven_aiohttp
except ImportError:  # pragma: no cover
    raven_aiohttp = None  # noqa

if typing.TYPE_CHECKING:
    from raven.handlers.logging import SentryHandler as _SentryHandler
else:
    class _SentryHandler: ...   # noqa: E701

__all__ = ['handler_from_dsn', 'setup']

DEFAULT_LEVEL: int = logging.WARNING


def _build_sentry_handler() -> Type[_SentryHandler]:
    from raven.handlers import logging as _logging

    class FaustSentryHandler(_logging.SentryHandler):  # type: ignore
        # 1) We override SentryHandler to write internal log messages
        #    mto sys.__stderr__ instead of sys.stderr, as the latter
        #    is redirected to a logger, causing noisy internal logs
        #    to be sent to Sentry.
        #
        # 2) We augment can_record to skip logging for CancelledError
        #    exceptions that have a ``.is_expected`` attribute set to True.
        #    That way once a CancelledError showing up in Sentry is
        #    investigated and resolved, we can silence that particular
        #    error in the future.
        def can_record(self, record: logging.LogRecord) -> bool:
            return (
                super().can_record(record) and
                not self._is_expected_cancel(record)
            )

        def _is_expected_cancel(self, record: logging.LogRecord) -> bool:
            # Returns true if this log record is associated with a
            # CancelledError.is_expected exception.
            if record.exc_info and record.exc_info[0] is not None:
                return bool(
                    issubclass(record.exc_info[0], asyncio.CancelledError) and
                    getattr(record.exc_info[1], 'is_expected', True))
            return False

        def emit(self, record: logging.LogRecord) -> None:
            try:
                self.format(record)

                if self.can_record(record):
                    self._emit(record)
                else:
                    self.carp(record.message)
            except Exception:
                if self.client.raise_send_errors:
                    raise
                self.carp('Top level Sentry exception caught - failed '
                          'creating log record')
                self.carp(record.msg)
                self.carp(traceback.format_exc())

        def carp(self, obj: Any) -> None:
            print(_logging.to_string(obj), file=sys.__stderr__)

    return FaustSentryHandler


def handler_from_dsn(dsn: str = None,
                     workers: int = 5,
                     include_paths: Iterable[str] = None,
                     loglevel: int = None,
                     qsize: int = 1000,
                     **kwargs: Any) -> Optional[logging.Handler]:
    if raven is None:
        raise ImproperlyConfigured(
            'faust.contrib.sentry requires the `raven` library.')
    if raven_aiohttp is None:
        raise ImproperlyConfigured(
            'faust.contrib.sentry requires the `raven_aiohttp` library.')
    level: int = loglevel if loglevel is not None else DEFAULT_LEVEL
    if dsn:
        client = raven.Client(
            dsn=dsn,
            include_paths=include_paths,
            transport=partial(
                raven_aiohttp.QueuedAioHttpTransport,
                workers=workers,
                qsize=qsize,
            ),
            disable_existing_loggers=False,
            **kwargs)
        handler = _build_sentry_handler()(client)
        handler.setLevel(level)
        return handler
    return None


def setup(app: AppT, *,
          dsn: str = None,
          workers: int = 4,
          max_queue_size: int = 1000,
          loglevel: int = None) -> None:
    sentry_handler = handler_from_dsn(
        dsn=dsn,
        workers=workers,
        qsize=max_queue_size,
        loglevel=loglevel,
    )
    if sentry_handler is not None:
        if sentry_sdk is None or _sdk_aiohttp is None:
            raise ImproperlyConfigured(
                'faust.contrib.sentry requires the `sentry_sdk` library.')
        sentry_sdk.init(
            dsn=dsn,
            integrations=[_sdk_aiohttp.AioHttpIntegration()],
        )
        app.conf.loghandlers.append(sentry_handler)
