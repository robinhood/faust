#!/usr/bin/env python

from random import random
from datetime import datetime
import faust

app = faust.App('tz_unaware', broker='kafka://localhost:9092')


class Model(faust.Record, serializer='json'):
    random: float


tz_unaware_topic = app.topic('tz_unaware_topic', value_type=Model)


@app.agent(tz_unaware_topic)
async def consume(stream):
    async for record in stream:
        print(f'record: {record}')


@app.crontab('*/1 * * * *', on_leader=True)
async def publish_every_minute():
    print('-- We should send a message every minute --')
    print(f'Sending message at: {datetime.now()}')
    msg = Model(random=round(random(), 2))
    await tz_unaware_topic.send(value=msg)


if __name__ == '__main__':
    app.main()
