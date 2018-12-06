#!/usr/bin/env python

# This example needs pytz so that we can set the approprate timezone when
# setting the crontab

from random import random
from datetime import datetime
import faust
import pytz

app = faust.App('tz_aware', broker='kafka://localhost:9092')
pacific = pytz.timezone('US/Pacific')


class Model(faust.Record, serializer='json'):
    random: float


tz_aware_topic = app.topic('tz_aware_topic', value_type=Model)


@app.agent(tz_aware_topic)
async def consume(stream):
    async for record in stream:
        print(f'record: {record}')


@app.crontab('16 20 * * *', tz=pacific, on_leader=True)
async def publish_at_8_20_pm_pacific():
    print('-- This should be sent at 20:16 Pacific time --')
    print(f'Sending message at: {datetime.now()}')
    msg = Model(random=round(random(), 2))
    await tz_aware_topic.send(value=msg)


if __name__ == '__main__':
    app.main()
