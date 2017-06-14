import faust
from typing import Any, AsyncIterator

app = faust.App('faust.stress')
rpc_topic = app.topic('faust.stress.rpc')


@app.actor(rpc_topic, concurrency=10)
async def simple(it: AsyncIterator[Any]) -> AsyncIterator[Any]:
    async for value in it:
        print('VALUE: %r' % (value,))
        yield value
