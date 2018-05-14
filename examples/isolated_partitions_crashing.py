#!/usr/bin/env python
"""Withdrawal example.

Quickstart
==========

1) Start worker:

.. sourcecode:: console

    $ ./examples/simple.py worker -l info

2) Start sending example data:

    $ ./examples/simple.py produce
"""
import asyncio
import random
from collections import Counter
from datetime import datetime, timezone
from itertools import count
import faust
from faust.cli import option


class Withdrawal(faust.Record, isodates=True, serializer='json'):
    user: str
    country: str
    amount: float
    date: datetime = None


app = faust.App(
    'faust-withdrawals4',
    broker='kafka://127.0.0.1:9092',
    store='rocksdb://',
    origin='examples.withdrawals',
    topic_partitions=4,
    worker_redirect_stdouts=False,
)
withdrawals_topic = app.topic('withdrawals99', value_type=Withdrawal)
foo_topic = app.topic('withdrawals5', value_type=Withdrawal)

counts = Counter()
previous_count = None


@app.agent(withdrawals_topic, isolated_partitions=True)
async def track_user_withdrawal(withdrawals):
    i = 0
    async for _withdrawal in withdrawals:  # noqa
        event = withdrawals.current_event
        assert event.message.tp in withdrawals.active_partitions
        print('GOT WITHDRAWAL FOR %r' % (event.message.tp,))
        i += 1
        counts[event.message.tp] += 1
        if not i % 2:
            print('RAISING FOR PARTITION: %r' % (
                withdrawals.active_partitions,))
            raise KeyError('OH NO')


@app.timer(3.0)
async def verify():
    global previous_count
    if previous_count is None:
        previous_count = Counter()
        previous_count.update(counts)
    else:
        from pprint import pprint
        pprint(dict(counts))
        from time import sleep
        sleep(2)


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
