"""

Example Usage:

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
        return (
            await event for event in (suspicious_countries.field.user &
                                      user_withdrawals.field.user)
            if event.withdrawal.amount > 500
        )

    async def main():
        app = faust.App()

        suspcious_users = app.add_stream(filter_suspicious_countries)
        s2 = app.add_stream(user_withdrawals)
        app.add_task(suspicious_users(suspicious_users, s2))

        suspicious_events[userid]  # Can use as dictionary
        user_withdrawals[userid]   # Same with tables
"""
import asyncio
from typing import Sequence
from .types import AppT
from .utils.service import Service

DEFAULT_SERVER = 'localhost:9092'


# TODO AutoOffsetReset

class Worker(Service):
    """Stream processing worker.

    Keyword Arguments:
        servers: List of server host/port pairs.
            Default is ``["localhost:9092"]``.
        loop: Provide specific asyncio event loop instance.
    """

    def __init__(self,
                 app: AppT,
                 *,
                 servers: Sequence[str] = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        super().__init__(loop=loop)
        self.app = app
        self.servers = servers or [DEFAULT_SERVER]

    async def on_start(self) -> None:
        await self.app.start()

    async def on_stop(self) -> None:
        await self.app.stop()
