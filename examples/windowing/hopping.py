#!/usr/bin/env python

# In this exapmple we have a function `publish_every_2secs` publishing a
# message every 2 senconds to topic `hopping_topic`
# We have created an agent `print_windowed_events` consuming events from
# `hopping_topic` that mutates the windowed table `hopping_table`

# `hopping_table` is a table with tumbling (overlaping) windows. Each of
# its windows is 10 seconds of duration, and we create a new window every 5
# seconds.
# |----------|
#       |-----------|
#             |-----------|
#                   |-----------|
# Since we produce an event every 2 seconds and our windows are 10
# seconds of duration we expect different the following results per method
# called in `WindowWrapper`:
# - now(): Gets the closest value to current local time. After 6 seconds it
# will always be between 3 and 5
# - current(): Gets the value relative to the event's timestamp. After 6
# seconds it will always be between 3 and 5
# - value(): Gets the value relative to default relative option. After 6
# seconds it will always be between 3 and 5
# - delta(30): Gets the value of window 30 secs before the current event. For
# the first 30 seconds it will be 0 and after second 40 it will always be 5.

from random import random
from datetime import timedelta
import faust

app = faust.App('windowing', broker='kafka://localhost:9092')


class Model(faust.Record, serializer='json'):
    random: float


TOPIC = 'hopping_topic'

hopping_topic = app.topic(TOPIC, value_type=Model)
hopping_table = app.Table(
    'hopping_table',
    default=int).hopping(10, 5, expires=timedelta(minutes=10))


@app.agent(hopping_topic)
async def print_windowed_events(stream):
    async for _ in stream: # noqa
        hopping_table['counter'] += 1
        value = hopping_table['counter']
        print(f'-- New Event (every 2 secs) written to hopping(10, 5) --')
        print(f'now() should have values between 1-3: {value.now()}')
        print(f'current() should have values between 1-3: {value.current()}')
        print(f'value() should have values between 1-3: {value.value()}')
        print(f'delta(30) should start at 0 and after 40 secs be 5: '
              f'{value.delta(30)}')


@app.timer(2.0, on_leader=True)
async def publish_every_2secs():
    msg = Model(random=round(random(), 2))
    await hopping_topic.send(value=msg)


if __name__ == '__main__':
    app.main()
