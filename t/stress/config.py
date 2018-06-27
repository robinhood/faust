import logging
import os
from typing import List
from raven import Client
from raven.handlers.logging import SentryHandler
from raven_aiohttp import AioHttpTransport

__all__ = [
    'broker',
    'store',
    'topic_partitions',
    'sentry_dsn',
    'loghandlers',
]

broker: str = os.environ.get('STRESS_BROKER', 'kafka://127.0.0.1:9092')
store: str = os.environ.get('STRESS_STORE', 'rocksdb://')
topic_partitions: int = int(os.environ.get('STRESS_PARTITIONS', 4))
sentry_dsn: str = os.environ.get('SENTRY_DSN')


def loghandlers() -> List[SentryHandler]:
    handlers = []
    if sentry_dsn:
        client = Client(
            dsn=sentry_dsn,
            include_paths=[__name__.split('.', 1)[0]],
            transport=AioHttpTransport,
            disable_existing_loggers=False,
        )
        handler = SentryHandler(client)
        handler.setLevel(logging.ERROR)
        handler.propagate = False
        handlers.append(handler)
    return handlers
