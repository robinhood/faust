import asyncio
import faust


class Event(faust.Event):
    value: int


topic = faust.topic('foo.bar.baz', type=Event)


@faust.stream(topic)
async def stream(it):
    return (event async for event in it if not event.value % 2)


async def slurp_stream():
    while 1:
        x = await stream.__anext__()
        print('Stream received: %r' % (x,))
        await asyncio.sleep(0.2)


async def producer():
    for i in range(100):
        await stream.send(Event(i))
        await asyncio.sleep(0.1)


async def main():
    await asyncio.gather(producer(), slurp_stream())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
