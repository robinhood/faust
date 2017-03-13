import asyncio
import faust

BLACKLIST = {'KP'}


class Event(faust.Event):
    account: str
    user: str
    country: str


class Withdrawal(faust.Event):
    amount: Decimal

all_events = faust.topic(pattern=r'.*', type=Event)
withdrawals = faust.topic(pattern=r'withdrawal\..*', type=Withdrawal)


@faust.stream(all_events, group_by=Event.user)
async def filter_suspicious_countries(it: Stream) -> Stream:
    return (event async for event in it if event.country in BLACKLIST)


@faust.aggregate(timedelta(days=2))
@faust.count(withdrawals)
def user_withdrawals(withdrawal: Withdrawl) -> Tuple[str, Decimal]:
    return withdrawal.user, withdrawal.amount


@faust.task()
async def suspicious_users(
        suspicious_countries: Stream,
        user_withdrawals: Table) -> StreamT:
    async for event in (suspicious_countries.field.user &
                        user_withdrawals.field.user):
        if event.withdrawal.amount > 500:
            yield event

async def main():
    app = faust.App()

    suspcious_users = app.add_stream(filter_suspicious_countries)
    s2 = app.add_stream(user_withdrawals)
    app.add_task(suspicious_users(suspicious_users, s2))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
