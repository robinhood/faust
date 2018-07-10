#!/usr/bin/env python
from typing import AsyncIterable
import faust
from faust import StreamT


app = faust.App('RPC99', reply_create_topic=True)
pow_topic = app.topic('RPC__pow')
mul_topic = app.topic('RPC__mul')


@app.agent(pow_topic)
async def pow(stream: StreamT[float]) -> AsyncIterable[float]:
    async for value in stream:
        yield await mul.ask(value=value ** 2)


@app.agent(mul_topic)
async def mul(stream: StreamT[float]) -> AsyncIterable[float]:
    async for value in stream:
        yield value * 100.0


@app.timer(interval=10.0)
async def _sender() -> None:
    # join' gives list with order preserved.
    res = await pow.join([30.3, 40.4, 50.5, 60.6, 70.7, 80.8, 90.9])
    print(f'JOINED: {res!r}')

    # map' gives async iterator to stream results (unordered)
    #   note: the argument can also be an async iterator.
    async for value in pow.map([30.3, 40.4, 50.5, 60.6, 70.7, 80.8, 90.9]):
        print(f'RECEIVED REPLY: {value!r}')

if __name__ == '__main__':
    app.main()
