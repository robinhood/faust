import asyncio
import faust
import os
import sys


class Withdrawal(faust.Record, serializer='json'):
    user: str
    country: str
    amount: float


app = faust.App(
    'f-simple',
    url='kafka://localhost:9092',
)
withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)

user_to_total = app.table('user_to_total', default=int)
country_to_total = app.table('country_to_total',
                             default=int).tumbling(10.0, expires=10.0)


@app.actor(withdrawals_topic)
async def find_large_withdrawals(withdrawals):
    async for withdrawal in withdrawals.group_by(Withdrawal.user):
        user_to_total[withdrawal.user] += withdrawal.amount
        country_to_total[withdrawal.country] += withdrawal.amount
        print('{!r} User Total: {!r}, Country Total: {!r}'.format(
            withdrawal,
            user_to_total[withdrawal.user],
            country_to_total[withdrawal.country].current(),
        ))


async def _publish_withdrawals():
    for i in range(10_000):
        print(f'+SEND {i!r}')
        await withdrawals_topic.send(
            b'K', Withdrawal(user='foo', amount=100.3 + i, country='FOO'))
        print(f'-SEND {i!r}')
    await withdrawals_topic.send(
        b'K', Withdrawal(user='foo', amount=999999.0, country='BAR'))
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
    except KeyError as exc:
        print(f'Unknown command: {exc}')
        raise SystemExit(os.EX_USAGE)
    except IndexError:
        print(f'Missing command. Try one of: {", ".join(COMMANDS)}')
        raise SystemExit(os.EX_USAGE)
    else:
        COMMANDS[command](loop=loop)
    finally:
        loop.close()


if __name__ == '__main__':
    main()
