import asyncio
import faust


class Event(faust.Event):
    value: int


topic = faust.topic('foo.bar.baz', type=Event)


@faust.stream(topic)
async def even(it: faust.Stream) -> faust.Stream:
    return (event async for event in it if not event.value % 2)


async def slurp_stream(s: faust.Stream):
    async for x in s:
        print('Stream received: {0!r}'.format(x))
        await asyncio.sleep(0.2)


async def producer(s: faust.Stream):
    for i in range(100):
        await s.send(Event(i))
        await asyncio.sleep(0.1)


async def main():
    app = faust.App()
    s = app.add_stream(even)
    await asyncio.gather(producer(s), slurp_stream(s))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
