from typing import AsyncIterator
import faust


app = faust.App('RPC99', create_reply_topic=True)
pow_topic = app.topic('RPC__pow')
mul_topic = app.topic('RPC__mul')


@app.actor(pow_topic)
async def pow(stream: AsyncIterator[float]) -> AsyncIterator[float]:
    async for value in stream:
        yield await mul.ask(value=value ** 2)


@app.actor(mul_topic)
async def mul(stream: AsyncIterator[float]) -> AsyncIterator[float]:
    async for value in stream:
        yield value * 100.0


@app.timer(interval=10.0)
async def _sender():
    # join' gives list with order preserved.
    res = await pow.join([30.3, 40.4, 50.5, 60.6, 70.7, 80.8, 90.9])
    print(f'JOINED: {res!r}')

    # map' gives async iterator to stream results (unordered)
    #   note: the argument can also be an async iterator.
    async for value in pow.map([30.3, 40.4, 50.5, 60.6, 70.7, 80.8, 90.9]):
        print(f'RECEIVED REPLY: {value!r}')
