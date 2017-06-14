import faust
from typing import AsyncIterator


app = faust.App('RPC99')
pow_topic = app.topic('RPC__pow')
mul_topic = app.topic('RPC__mul')


@app.actor(pow_topic)
async def pow(stream: AsyncIterator[float]) -> AsyncIterator[float]:
    async for value in stream:
        print(f'POW RECEIVED: {value!r}')
        yield await mul.ask(value=value ** 2)


@app.actor(mul_topic)
async def mul(stream: AsyncIterator[float]) -> AsyncIterator[float]:
    async for value in stream:
        print(f'MUL RECEIVED: {value!r}')
        yield value * 100.0


@app.timer(interval=10.0)
async def _sender():
    print(await pow.ask(value=30.3))
