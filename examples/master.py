import faust

app = faust.App(
    'master-example',
    url='kafka://localhost:9092',
    value_serializer='raw',
)


@app.timer(3.0)
async def print_everywhere():
    print('HELLO!')


@app.timer(3.0, on_master=True)
async def print_on_master():
    print('HELLO FROM MASTER!')


@app.actor()
async def foo(bar):
    async for msg in bar:
        print(msg)
