#!/usr/bin/env python
"""Stress test suite.

Quickstart
==========

1) Start worker:

.. sourcecode:: console

    $ ./examples/stress.py worker -l info

2) Start sending example data:

    $ ./examples/stress.py produce
"""
import asyncio
import logging
import os
import random
from datetime import datetime, timezone
from itertools import count
from typing import List
import faust
from faust.cli import option
from raven import Client
from raven.handlers.logging import SentryHandler
from raven_aiohttp import AioHttpTransport


class Withdrawal(faust.Record, isodates=True, serializer='json'):
    user: str
    country: str
    amount: float
    date: datetime = None


def generate_handlers() -> List[SentryHandler]:
    handlers = []
    dsn = os.environ.get('SENTRY_DSN')
    if dsn:
        client = Client(
            dsn=dsn,
            include_paths=[__name__.split('.', 1)[0]],
            transport=AioHttpTransport,
            disable_existing_loggers=False,
        )
        handler = SentryHandler(client)
        handler.setLevel(logging.WARNING)
        handler.propagate = False
        handlers.append(handler)
    return handlers


app = faust.App(
    'faust-withdrawals30',
    broker=os.environ.get('STRESS_BROKER', 'kafka://127.0.0.1:9092'),
    store=os.environ.get('STRESS_STORE', 'rocksdb://'),
    origin='t.stress',
    topic_partitions=os.environ.get('STRESS_PARTITIONS', 4),
    loghandlers=generate_handlers(),
)
withdrawals_topic = app.topic('withdrawals130', value_type=Withdrawal)

# withdrawal_counts = app.Table('withdrawal-counts', default=int)
seen_events = 0


@app.agent(withdrawals_topic,
           isolated_partitions=False)
async def track_user_withdrawal(withdrawals):
    global seen_events
    i = 0
    async for _withdrawal in withdrawals:  # noqa
        i += 1
        seen_events += 1
        # Uncomment to raise exceptions during processing
        # to test agents restarting.
        if not i % 100:
            raise KeyError('OH NO')


@app.task
async def report_progress(app):
    prev_count = 0
    while not app.should_stop:
        await app._service.sleep(5.0)
        if seen_events <= prev_count:
            print(f'Not progressing: was {prev_count} now {seen_events}')
        else:
            print(f'Progressing: was {prev_count} now {seen_events}')
        prev_count = seen_events


@app.command(
    option('--max-latency',
           type=float, default=0.5, envvar='PRODUCE_LATENCY',
           help='Add delay of (at most) n seconds between publishing.'),
    option('--max-messages',
           type=int, default=None,
           help='Send at most N messages or 0 for infinity.'),
)
async def produce(self, max_latency: float, max_messages: int):
    """Produce example Withdrawal events."""
    for i, withdrawal in enumerate(generate_withdrawals(max_messages)):
        await withdrawals_topic.send(key=withdrawal.user, value=withdrawal)
        if not i % 10000:
            self.say(f'+SEND {i}')
        if max_latency:
            await asyncio.sleep(random.uniform(0, max_latency))


def generate_withdrawals(n: int = None):
    for d in generate_withdrawals_dict(n):
        yield Withdrawal(**d)


def generate_withdrawals_dict(n: int = None):
    num_countries = 5
    countries = [f'country_{i}' for i in range(num_countries)]
    country_dist = [0.9] + ([0.10 / num_countries] * (num_countries - 1))
    num_users = 500
    users = [f'user_{i}' for i in range(num_users)]
    for _ in range(n) if n is not None else count():
        yield {
            'user': random.choice(users),
            'amount': random.uniform(0, 25_000),
            'country': random.choices(countries, country_dist)[0],
            'date': datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        }


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        sys.argv.extend(['worker', '-l', 'info'])
    app.main()
