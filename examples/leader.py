#!/usr/bin/env python
import random
import faust


app = faust.App(
    'leader-example',
    broker='kafka://localhost:9092',
    value_serializer='raw',
)


@app.timer(2.0, on_leader=True)
async def publish_greetings():
    print('PUBLISHING ON LEADER!')
    await say.send(value=str(random.random()))


@app.agent()
async def say(greetings):
    async for greeting in greetings:
        print(greeting)
