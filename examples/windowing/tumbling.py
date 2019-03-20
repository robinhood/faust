#!/usr/bin/env python

# In this exapmple we have a function `publish_every_2secs` publishing a
# message every 2 senconds to topic `tumbling_topic`
# We have created an agent `print_windowed_events` consuming events from
# `tumbling_topic` that mutates the windowed table `tumbling_table`

# `tumbling_table` is a table with tumbling (non overlaping) windows. Each of
# its windows is 10 seconds of duration. The table counts the number of events
# per window. Since we produce an event every 2 seconds and our windows are 10
# seconds of duration we expect different the following results per method
# called in `WindowWrapper`:
# - now(): Gets the closest value to current local time. From 1 to 5
# - current(): Gets the value relative to the event's timestamp. From 1 to 5.
# - value(): Gets the value relative to default relative option
# - delta(30): Gets the value of window 30 secs before the current event. For
# the first 30 seconds it will be 0 and after second 40 it will always be 5.

from random import random
from datetime import timedelta
import faust

app = faust.App('windowing', broker='kafka://localhost:9092')


class Model(faust.Record, serializer='json'):
    random: float


TOPIC = 'tumbling_topic'

tumbling_topic = app.topic(TOPIC, value_type=Model)
tumbling_table = app.Table(
    'tumbling_table',
    default=int).tumbling(10, expires=timedelta(minutes=10))


@app.agent(tumbling_topic)
async def print_windowed_events(stream):
    async for _ in stream: # noqa
        tumbling_table['counter'] += 1

        value = tumbling_table['counter']

        print('-- New Event (every 2 secs) written to tumbling(10) --')
        print(f'now() should go from 1 to 5: {value.now()}')
        print(f'current() should go from 1 to 5: {value.current()}')
        print(f'value() should go from 1 to 5: {value.value()}')
        print('delta(30) should start at 0 and after 40 secs be 5: '
              f'{value.delta(30)}')


@app.timer(2.0, on_leader=True)
async def publish_every_2secs():
    msg = Model(random=round(random(), 2))
    await tumbling_topic.send(value=msg)


if __name__ == '__main__':
    app.main()
