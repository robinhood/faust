import faust

app = faust.App(
    'tableofset',
    origin='examples.tableofset',
)

table = app.SetTable('people', value_type=str)

joining_topic = app.topic('people_joining', value_type=str)
leaving_topic = app.topic('people_leaving', value_type=str)


@app.agent(joining_topic)
async def join(stream):
    async for key, name in stream.items():
        table[key].add(name)


@app.agent(leaving_topic)
async def leave(stream):
    async for key, name in stream.items():
        table[key].discard(name)


@app.timer(1.0)
async def _joiners():
    await joining_topic.send(key="foo", value="Ask")


if __name__ == '__main__':
    app.main()
