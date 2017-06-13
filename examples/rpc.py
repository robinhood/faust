import faust
from typing import AsyncIterator


class Log(faust.Record, serializer='json'):
    account: str
    value: float



app = faust.App('RPC')
topic = app.topic('RPC', value_type=Log)


@app.actor(topic)
async def foo(stream: AsyncIterator[Log]) -> AsyncIterator[float]:
    async for event in stream:
        print('RECEIVED: %r' % (event,))
        yield event.value ** 2


@app.timer(interval=10.0)
async def _sender():
    print(await foo.ask(value=Log(account='foo', value=30.3)))
