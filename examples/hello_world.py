import faust

app = faust.App('hello-world', url='kafka://localhost:9092')

greetings_topic = app.topic('greetings', value_type=str)


@app.actor(greetings_topic)
async def print_greetings(greetings):
    async for greeting in greetings:
        print(greeting)
