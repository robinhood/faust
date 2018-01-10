#!/usr/bin/env python
import asyncio
import aiohttp
import faust


class Record(faust.Record):
    value: int


app = faust.App('concurrency', broker='kafka://localhost')
topic = app.topic('concurrency', value_type=Record)


@app.agent(topic, concurrency=200)
async def mytask(records):
    session = aiohttp.ClientSession()
    async for record in records:
        await session.get(f'http://www.google.com/?#q={record.value}')


async def producer():
    for i in range(10_000):
        await topic.send(value=Record(value=i))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(producer())
    loop.stop()
