import asyncio
import faust
import os
import sys
from faust.sensors import Sensor


class Withdrawal(faust.Record, serializer='json'):
    amount: float


topic = faust.topic('mytopic', value_type=Withdrawal)


# -- Stream is coroutine

async def combine_withdrawals(it):
    while 1:
        eventA = await it.next()
        try:
            eventB = await asyncio.wait_for(it.next(), 2.0)
        except asyncio.TimeoutError:
            yield eventA
        else:
            yield eventA.derive(amount=eventA.amount + eventB.amount)


async def find_large_withdrawals(app):
    withdrawals = app.stream(topic, combine_withdrawals)
    print('CURRENT_APP IS: %r' % (app.current_app(),))
    async for withdrawal in withdrawals:
        print('TASK GENERATOR RECV FROM OUTBOX: %r' % (withdrawal,))
        if withdrawal.amount > 9999.0:
            print('ALERT: large withdrawal: {0.amount!r}'.format(withdrawal))


app = faust.App('myid', url='kafka://localhost:9092')


async def produce():
    async with app:
        for i in range(100):
            print('+SEND %r' % (i,))
            await app.send(topic, None, Withdrawal(amount=100.3 + i))
            print('-SEND %r' % (i,))
        await app.send(topic, None, Withdrawal(amount=999999.0))
        await asyncio.sleep(30)


async def consume():
    app.add_task(find_large_withdrawals(app))
    worker = faust.Worker(app, loglevel='INFO')
    worker.sensors.add(Sensor())
    await worker.start()


COMMANDS = {
    'consume': consume,
    'produce': produce,
}


async def main():
    try:
        await COMMANDS[sys.argv[1]]()
    except KeyError as exc:
        print('Unknown command: {0}'.format(exc))
        raise SystemExit(os.EX_USAGE)
    except IndexError:
        print('Missing command. Try one of: {0}'.format(', '.join(COMMANDS)))
        raise SystemExit(os.EX_USAGE)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    if len(sys.argv) > 1 and sys.argv[1] == 'consume':
        loop.run_forever()
    loop.close()
