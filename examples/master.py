import faust
import random


app = faust.App(
    'master-example',
    url='kafka://localhost:9092',
    value_serializer='raw',
)


@app.timer(2.0, on_master=True)
async def publish_greetings():
    print('PUBLISHING ON MASTER!')
    await say.send(value=str(random.random()))


@app.actor()
async def say(greetings):
    async for greeting in greetings:
        print(greeting)
