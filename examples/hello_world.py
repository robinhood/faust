#!/usr/bin/env python
import faust

app = faust.App(
    'hello-world',
    url='kafka://localhost:9092',
    value_serializer='raw',
)

greetings_topic = app.topic('greetings')


@app.agent(greetings_topic)
async def print_greetings(greetings):
    async for greeting in greetings:
        print(greeting)

if __name__ == '__main__':
    app.main()
