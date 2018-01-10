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
from datetime import datetime, timezone
from itertools import count
import faust
from faust.cli import option
from faust.types import EventT, TP


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
    default_partitions=4,
)
withdrawals_topic = app.topic('withdrawals2', value_type=Withdrawal)


async def print_key_value(event: EventT) -> None:
    tp = TP(topic=event.message.topic, partition=event.message.partition)
    if not app.assignor.is_active(tp):
        print(f'{event.key}:{event.value}')

user_to_total = app.Table(
    'user_to_total', default=int, on_changelog_event=print_key_value,
    standby_buffer_size=1, recovery_buffer_size=200,
).tumbling(3600).relative_to_stream()
country_to_total = app.Table(
    'country_to_total', default=int).tumbling(10.0, expires=10.0)


@app.agent(withdrawals_topic)
async def find_large_user_withdrawals(withdrawals):
    async for withdrawal in withdrawals:
        user_to_total[withdrawal.user] += withdrawal.amount


@app.command(
    option('--max-latency',
           type=float, default=PRODUCE_LATENCY,
           help='Add delay of (at most) n seconds between publishing.'),
    option('--max-messages',
           type=int, default=None,
           help='Send at most N messages or 0 for infinity.'),
)
async def produce(self, max_latency: float, max_messages: int):
    """Produce example Withdrawal events."""
    num_countries = 5
    countries = [f'country_{i}' for i in range(num_countries)]
    country_dist = [0.9] + ([0.10 / num_countries] * (num_countries - 1))
    num_users = 500
    users = [f'user_{i}' for i in range(num_users)]
    self.say('Done setting up. SENDING!')
    for i in range(max_messages) if max_messages is not None else count():
        withdrawal = Withdrawal(
            user=random.choice(users),
            amount=random.uniform(0, 25_000),
            country=random.choices(countries, country_dist)[0],
            date=datetime.utcnow().replace(tzinfo=timezone.utc),
        )
        await withdrawals_topic.send(key=withdrawal.user, value=withdrawal)
        if not i % 10000:
            self.say(f'+SEND {i}')
        if max_latency:
            await asyncio.sleep(random.uniform(0, max_latency))


if __name__ == '__main__':
    app.main()
