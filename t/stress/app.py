from typing import Any, AsyncIterator
import faust

app = faust.App('faust.stress')
rpc_topic = app.topic('faust.stress.rpc')


@app.agent(rpc_topic, concurrency=10)
async def simple(it: AsyncIterator[Any]) -> AsyncIterator[Any]:
    async for value in it:
        yield value
