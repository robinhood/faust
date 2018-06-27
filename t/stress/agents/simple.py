from .. import topics
from ..app import app

__all__ = ['track_user_withdrawal']

# withdrawal_counts = app.Table('withdrawal-counts', default=int)
seen_events = 0


@app.agent(topics.withdrawals)
async def track_user_withdrawal(withdrawals):
    global seen_events
    i = 0
    async for _withdrawal in withdrawals:  # noqa
        i += 1
        seen_events += 1


@app.task
async def report_progress(app):
    prev_count = 0
    while not app.should_stop:
        await app._service.sleep(5.0)
        if seen_events <= prev_count:
            print(f'Simple not progressing: '
                  f'was {prev_count} now {seen_events}')
        else:
            print(f'Simple Progressing: was {prev_count} now {seen_events}')
        prev_count = seen_events
