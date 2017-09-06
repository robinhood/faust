import asyncio
import os
import random
import sys
from time import monotonic
import faust

PRODUCE_LATENCY = float(os.environ.get('PRODUCE_LATENCY', 0.5))


class Withdrawal(faust.Record, serializer='json'):
    user: str
    country: str
    amount: float


app = faust.App(
    'f-simple',
    url='kafka://127.0.0.1:9092',
    store='rocksdb://',
    default_partitions=6,
)
withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)

user_to_total = app.Table('user_to_total', default=int)
country_to_total = app.Table(
    'country_to_total', default=int).tumbling(10.0, expires=10.0)


@app.actor(withdrawals_topic, concurrency=1)
async def find_large_user_withdrawals(withdrawals):
    events = 0
    time_start = monotonic()
    time_first_start = monotonic()
    async for withdrawal in withdrawals.through('bar'):
        events += 1
        if not events % 10_000:
            time_now = monotonic()
            print('TIME PROCESSING 10k: %r' % (
                time_now - time_start))
            time_start = time_now
        if not events % 100_000:
            time_now = monotonic()
            print('----TIME PROCESSING 100k: %r' % (
                time_now - time_first_start))
            time_first_start = time_now
        user_to_total[withdrawal.user] += withdrawal.amount


#@app.actor(withdrawals_topic)
#async def find_large_country_withdrawals(withdrawals):
#    async for withdrawal in withdrawals.group_by(Withdrawal.country):
#        country_to_total[withdrawal.country] += withdrawal.amount


async def _publish_withdrawals():
    num_countries = 5
    countries = [f'country_{i}' for i in range(num_countries)]
    country_dist = [0.9] + ([0.10 / num_countries] * (num_countries - 1))
    num_users = 500
    users = [f'user_{i}' for i in range(num_users)]
    print('Done setting up. SENDING!')
    i = 0
    while True:
        i += 1
        withdrawal = Withdrawal(
            user=random.choice(users),
            amount=random.uniform(0, 25_000),
            country=random.choices(countries, country_dist)[0],
        )
        await withdrawals_topic.send(key=withdrawal.user, value=withdrawal)
        if not i % 10000:
            print(f'+SEND {i}')
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
