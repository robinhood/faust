from decimal import Decimal
import faust
from faust.types import StreamT
from faustapp import app
from .models import Account


class AccountRecord(faust.Record):
    name: str
    score: float
    active: bool


@app.agent()
async def add_account(accounts: StreamT[AccountRecord]):
    async for account in accounts:
        print('GOT RECORD: %r' % (account,))
        result = Account.objects.create(
            name=account.name,
            score=Decimal(str(account.score)),
            active=account.active,
        )
        yield result.pk


@app.agent()
async def disable_account(account_ids: StreamT[int]):
    async for account_id in account_ids:
        account = Account.objects.get(pk=account_id)
        account.active = False
        account.save()
        yield account.active
