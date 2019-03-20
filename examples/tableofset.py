import faust
from faust.cli import argument

app = faust.App(
    'table-of-sets-windowed',
    origin='examples.tableofset',
    topic_partitions=4,
    version=2,
)

table = app.SetTable(
    'people', value_type=str,
)

joining_topic = app.topic('people_joining2', key_type=str, value_type=str)
leaving_topic = app.topic('people_leaving2', key_type=str, value_type=str)


@app.agent(joining_topic)
async def join(stream):
    async for key, name in stream.items():
        print(f'- {name.capitalize()} joined {key}')
        table[key].add(name)
        print(f'TABLE CUR: {table[key]!r}')


@app.agent(leaving_topic)
async def leave(stream):
    async for key, name in stream.items():
        print(f'- {name.capitalize()} left {key}')
        table[key].discard(name)
        print(f'TABLE CUR: {table[key]!r}')


@app.command(
    argument('location'),
    argument('name'),
)
async def joining(self, location: str, name: str):
    await joining_topic.send(key=location, value=name)


@app.command(
    argument('location'),
    argument('name'),
)
async def leaving(self, location: str, name: str):
    await leaving_topic.send(key=location, value=name)


@app.timer(10.0)
async def _dump():
    print(f'TABLE NOW:\n{table.as_ansitable()}')


if __name__ == '__main__':
    app.main()
