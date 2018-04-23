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
)
withdrawals_topic = app.topic('withdrawals4', value_type=Withdrawal)

user_to_total = app.Table(
    'user_to_total', default=int,
).tumbling(3600).relative_to_stream()

country_to_total = app.Table(
    'country_to_total', default=int,
).tumbling(10.0, expires=10.0).relative_to_stream()


@app.agent(withdrawals_topic)
async def track_user_withdrawal(withdrawals):
    async for withdrawal in withdrawals:
        user_to_total[withdrawal.user] += withdrawal.amount


@app.agent()
async def track_country_withdrawal(withdrawals):
    async for withdrawal in withdrawals.group_by(Withdrawal.country):
        country_to_total[withdrawal.country] += withdrawal.amount


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


def generate_withdrawals(n: int = None):
    for d in generate_withdrawals_dict(n):
        yield Withdrawal(**d)


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        sys.argv.extend(['worker', '-l', 'info'])
    app.main()
