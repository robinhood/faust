import asyncio
import faust
import io
import os
import sys
from faust.sensors import Sensor

GRAPH = os.environ.get('GRAPH')


class Withdrawal(faust.Record, serializer='json'):
    user: str
    amount: float


topic = faust.topic('f-simple', value_type=Withdrawal)
app = faust.App(
    'f-simple',
    url='kafka://localhost:9092',
)


@app.task
async def find_large_withdrawals(app):
    if GRAPH:
        asyncio.ensure_future(_dump_beacon(app))
    withdrawals = app.stream(topic)
    user_to_total = withdrawals.aggregate(
        "user_to_total", lambda total, withdrawal: total + withdrawal.amount,
        default=int)
    async for key, withdrawal in withdrawals.items():
        print('%r Withdrawal: %r' % (key, withdrawal,))
        print(user_to_total[key])


async def _dump_beacon(app):
    await asyncio.sleep(4)
    print(app.render_graph())


async def _publish_withdrawals():
    for i in range(100):
        print('+SEND %r' % (i,))
        await app.send(topic, b'K', Withdrawal(user='foo', amount=100.3 + i))
        print('-SEND %r' % (i,))
    await app.send(topic, b'K', Withdrawal(user='foo', amount=999999.0))
    await asyncio.sleep(30)


def produce(loop):
    loop.run_until_complete(_publish_withdrawals())


COMMANDS = {
    'consume': app.start,
    'produce': produce,
}


def main(loop=None):
    loop = loop or asyncio.get_event_loop()
    try:
        command = sys.argv.pop(1)
        COMMANDS[command](loop=loop)
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
