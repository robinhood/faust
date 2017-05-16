import asyncio
import aiohttp
import faust


class Record(faust.Record):
    value: int


app = faust.App('concurrency', url='kafka://localhost')
topic = app.topic('concurrency', value_type=Record)

@app.actor(topic, concurrency=200)
async def mytask(records):
    sleep = asyncio.sleep
    session = aiohttp.ClientSession()
    async for record in records:
        await session.get(
            'http://www.google.com/?#safe=off&q={}'.format(record.value))


async def producer():
    #await app.send_many(
    #    (topic, None, Record(value=i))
    #    for i in range(10_000)
    #)
    for i in range(10_000):
        await topic.send(value=Record(value=i))


if __name__ == '__main__':
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(producer())
    loop.stop()
