#!/usr/bin/env python
"""Withdrawal example.

Quick Start
===========

1) Start worker:

.. sourcecode:: console

    $ ./examples/simple.py worker -l info

2) Start sending example data:

    $ ./examples/simple.py produce
"""
import asyncio
import os
import random
from datetime import datetime
from itertools import count
from time import monotonic
import faust

PRODUCE_LATENCY = float(os.environ.get('PRODUCE_LATENCY', 0.5))

class Withdrawal(faust.Record, isodates=True, serializer='json'):
    user: str
    country: str
    amount: float
    date: datetime = None


app = faust.App(
    'f-simple2',
    url='kafka://127.0.0.1:9092',
    store='rocksdb://',
    default_partitions=6,
)
withdrawals_topic = app.topic('withdrawals2', value_type=Withdrawal)

user_to_total = app.Table(
    'user_to_total', default=int).tumbling(3600).relative_to_stream()
country_to_total = app.Table(
    'country_to_total', default=int).tumbling(10.0, expires=10.0)


@app.agent(withdrawals_topic)
async def find_large_user_withdrawals(withdrawals):
    events = 0
    time_start = monotonic()
    time_first_start = monotonic()
    async for withdrawal in withdrawals:
        events += 1
        if not events % 10_000:
            time_now = monotonic()
            print('TIME PROCESSING 10k: %r' % (
                time_now - time_start))
            time_start = time_now
        if not events % 100_000:
            time_now = monotonic()
            print('----TIME PROCESSING 100k: %r' % (
                time_now - time_first_start))
            time_first_start = time_now
        print('WITHDRAWAL: %r' % (withdrawal,))
        if withdrawal.user in user_to_total:
            print('ALREADY HAVE IT:')
            print(user_to_total[withdrawal.user].value())
        user_to_total[withdrawal.user] += withdrawal.amount


@user_to_total.on_recover
async def echo_size():
    await asyncio.sleep(1.0)
    print('Total size', len(user_to_total))


@app.command()
async def produce(self):
    'Produce example Withdrawal events.'
    num_countries = 5
    countries = [f'country_{i}' for i in range(num_countries)]
    country_dist = [0.9] + ([0.10 / num_countries] * (num_countries - 1))
    num_users = 500
    users = [f'user_{i}' for i in range(num_users)]
    self.say('Done setting up. SENDING!')
    for i in count():
        withdrawal = Withdrawal(
            user=random.choice(users),
            amount=random.uniform(0, 25_000),
            country=random.choices(countries, country_dist)[0],
            date=datetime.utcnow(),
        )
        await withdrawals_topic.send(key=withdrawal.user, value=withdrawal)
        if not i % 10000:
            self.say(f'+SEND {i}')
        if PRODUCE_LATENCY:
            await asyncio.sleep(random.uniform(0, PRODUCE_LATENCY))


if __name__ == '__main__':
    app.main()
