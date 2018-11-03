#!/usr/bin/env python

# In this exapmple we have a function `publish_every_2secs` publishing a
# message every 2 senconds to topiuc `window_topic`
# We have created an agent `print_windowed_events` consuming events from
# `window_topic` that mutates the windowed table `table`

# `table` is a table table with tumbling (non overlaping) windows. Each of its
# windows is 10 seconds of duration. The table counts the number of events per
# window. Since we produce an event every 2 seconds and our windows are 10
# seconds of duration we only expect values between 1 and 5.

from random import random
from datetime import timedelta
import faust

app = faust.App('windowing', broker='kafka://localhost:9092')


class Model(faust.Record, serializer='json'):
    random: float


TOPIC = 'window_topic'

window_topic = app.topic(TOPIC, value_type=Model)
table = app.Table('tumbling_now_table', default=int) \
            .tumbling(10, expires=timedelta(minutes=10))


@app.agent(window_topic)
async def print_windowed_events(stream):
    async for _ in stream: # noqa
        table['counter'] += 1
        print("Values should go from 1 to 5. Now window value: "
              f"{table['counter'].now()}")


@app.timer(2.0, on_leader=True)
async def publish_every_2secs():
    msg = Model(random=round(random(), 2))
    await window_topic.send(value=msg)
    print(f"Producer just published message: {msg}")


if __name__ == '__main__':
    app.main()
