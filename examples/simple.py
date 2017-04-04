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
    async for withdrawal in withdrawals.through('temp'):
        print('TASK GENERATOR RECV FROM OUTBOX: %r' % (withdrawal,))
        if withdrawal.amount > 9999.0:
            print('ALERT: large withdrawal: {0.amount!r}'.format(withdrawal))


app = faust.App('myid', url='kafka://localhost:9092')


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
    app.add_task(find_large_withdrawals(app))
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
        print('Unknown command: {0}'.format(exc))
        raise SystemExit(os.EX_USAGE)
    except IndexError:
        print('Missing command. Try one of: {0}'.format(', '.join(COMMANDS)))
        raise SystemExit(os.EX_USAGE)
    finally:
        loop.close()


if __name__ == '__main__':
    main()
