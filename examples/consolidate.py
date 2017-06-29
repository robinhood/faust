import asyncio
import os
import sys
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


@app.actor(topic)
async def find_large_withdrawals(stream):
    withdrawals = app.stream(stream, combine_withdrawals)
    async for w in withdrawals.through('foo'):
        if w.amount > 9999.0:
            print(f'ALERT: large withdrawal: {w.amount!r}')


async def _publish_withdrawals():
    for i in range(100):
        print('+SEND %r' % (i,))
        await app.send(topic, None, Withdrawal(amount=100.3 + i))
        print('-SEND %r' % (i,))
    await app.send(topic, None, Withdrawal(amount=999999.0))
    await asyncio.sleep(30)


def produce(loop):
    loop.run_until_complete(_publish_withdrawals())


def consume(loop):
    worker = faust.Worker(app, loglevel='INFO', loop=loop)
    worker.execute_from_commandline()
    loop.run_forever()


COMMANDS = {
    'consume': consume,
    'produce': produce,
}


def main(loop=None):
    loop = loop or asyncio.get_event_loop()
    try:
        COMMANDS[sys.argv[1]](loop=loop)
    except KeyError as exc:
        print(f'Unknown command: {exc}')
        raise SystemExit(os.EX_USAGE)
    except IndexError:
        print(f'Missing command. Try one of: {", ".join(COMMANDS)}')
        raise SystemExit(os.EX_USAGE)
    finally:
        loop.close()


if __name__ == '__main__':
    main()
