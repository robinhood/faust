#!/usr/bin/env python
import faust

app = faust.App(
    'hello-world',
    broker='kafka://localhost:9092',
)

greetings_topic = app.topic('greetings', value_type=str)


@app.agent(greetings_topic)
async def print_greetings(greetings):
    async for greeting in greetings:
        print(greeting)


@app.timer(5)
async def produce():
    for i in range(100):
        await print_greetings.send(value=f'hello {i}')

if __name__ == '__main__':
    app.main()
