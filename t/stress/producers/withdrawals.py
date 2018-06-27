import random
from datetime import datetime, timezone
from itertools import count
from .. import topics
from ..models import Withdrawal
from ..decorators import producer


@producer
async def produce_withdrawals(max_messages):
    for withdrawal in generate_withdrawals(max_messages):
        yield await topics.withdrawals.send(key=None, value=withdrawal)


def generate_withdrawals(n: int = None):
    for d in generate_withdrawals_dict(n):
        yield Withdrawal(**d)


def generate_withdrawals_dict(n: int = None):
    num_countries = 5
    countries = [f'country_{i}' for i in range(num_countries)]
    country_dist = [0.9] + ([0.10 / num_countries] * (num_countries - 1))
    num_users = 500
    users = [f'user_{i}' for i in range(num_users)]
    for _ in range(n) if n is not None else count():
        yield {
            'user': random.choice(users),
            'amount': random.uniform(0, 25_000),
            'country': random.choices(countries, country_dist)[0],
            'date': datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        }
