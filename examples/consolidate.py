import asyncio
import faust


class Withdrawal(faust.Record, serializer='json'):
    amount: float


app = faust.App('myid', url='kafka://localhost:9092')
topic = app.topic('mytopic', value_type=Withdrawal)

# -- Stream is coroutine


async def combine_withdrawals(it):
    while 1:
        eventA = await it.next()  # noqa: B305
        try:
            eventB = await asyncio.wait_for(it.next(), 2.0)  # noqa: B305
        except asyncio.TimeoutError:
            yield eventA
        else:
            yield eventA.derive(amount=eventA.amount + eventB.amount)


@app.agent(topic)
async def find_large_withdrawals(stream):
    withdrawals = app.stream(stream, combine_withdrawals)
    async for w in withdrawals.through('foo'):
        if w.amount > 9999.0:
            print(f'ALERT: large withdrawal: {w.amount!r}')


@app.command()
async def produce():
    """Produce example data."""
    for i in range(100):
        print('+SEND %r' % (i,))
        await app.send(topic, None, Withdrawal(amount=100.3 + i))
        print('-SEND %r' % (i,))
    await app.send(topic, None, Withdrawal(amount=999999.0))
    await asyncio.sleep(30)


if __name__ == '__main__':
    app.main()
