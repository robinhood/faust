import faust
from typing import AsyncIterator


class Log(faust.Record, serializer='json'):
    account: str
    value: float



app = faust.App('rpc-example')
topic = app.topic('rpc', value_type=Log)


@app.actor(topic)
async def foo(stream: AsyncIterator[Log]) -> AsyncIterator[float]:
    async for event in stream:
        yield event.value ** 2
