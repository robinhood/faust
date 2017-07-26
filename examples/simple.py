import asyncio
import os
import random
import string
import sys
import faust

PRODUCE_LATENCY = float(os.environ.get('PRODUCE_LATENCY', 0.5))


class Withdrawal(faust.Record, serializer='json'):
    user: str
    country: str
    amount: float


app = faust.App(
    'f-simple',
    url='kafka://localhost:9092',
    default_partitions=6,
)
withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)

user_to_total = app.Table('user_to_total', default=int)
country_to_total = app.Table(
    'country_to_total', default=int).tumbling(10.0, expires=10.0)


@app.actor(withdrawals_topic)
async def find_large_user_withdrawals(withdrawals):
    async for withdrawal in withdrawals:
        user_to_total[withdrawal.user] += withdrawal.amount


@app.actor(withdrawals_topic)
async def find_large_country_withdrawals(withdrawals):
    async for withdrawal in withdrawals.group_by(Withdrawal.country):
        country_to_total[withdrawal.country] += withdrawal.amount


async def _publish_withdrawals():
    num_countries = 5
    countries = [f'country_{i}' for i in range(num_countries)]
    country_dist = [0.9] + ([0.10/num_countries] * (num_countries - 1))
    num_users = 500
    users = [f'user_{i}' for i in range(num_users)]
    print('Done setting up. SENDING!')
    while True:
        withdrawal = Withdrawal(
            user=random.choice(users),
            amount=random.uniform(0, 25_000),
            country=random.choices(countries, country_dist)[0],
        )
        await withdrawals_topic.send(key=withdrawal.user, value=withdrawal)
        print(f'+SEND {withdrawal}')
        if PRODUCE_LATENCY:
            await asyncio.sleep(random.uniform(0, PRODUCE_LATENCY))


def produce(loop):
    loop.run_until_complete(_publish_withdrawals())


COMMANDS = {
    'consume': app.start_worker,
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
