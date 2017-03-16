import asyncio
import faust
import logging
import os
import sys
logging.basicConfig(level=logging.INFO)


class Withdrawal(faust.Event, serializer='json'):
    amount: float


topic = faust.topic('mytopic', type=Withdrawal)


@faust.stream(topic)
async def all_withdrawals(it):
    while 1:
        eventA = await it.next()
        try:
            eventB = await asyncio.wait_for(it.next(), 2.0)
        except asyncio.TimeoutError:
            yield eventA
        else:
            yield Withdrawal(amount=eventA.amount + eventB.amount)


async def find_large_withdrawals(withdrawals):
    async for withdrawal in withdrawals:
        print('TASK GENERATOR RECV FROM OUTBOX: %r' % (withdrawal,))
        if withdrawal.amount > 9999.0:
            print('ALERT: large withdrawal: {0.amount!r}'.format(withdrawal))


app = faust.App('aiokafka://localhost:9092')


async def produce():
    for i in range(100):
        await app.send(topic, None, Withdrawal(100.3 + i))
    await app.send(topic, None, Withdrawal(999999.0))


async def consume():
    withdrawals = app.add_stream(all_withdrawals)
    app.add_task(find_large_withdrawals(withdrawals))


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
    loop.run_forever()
    loop.close()
