from ...app import create_stress_app
from ...models import Withdrawal

app = create_stress_app(
    name='f-stress-forwarder',
    origin='t.stress.tests.forwarder',
    stream_buffer_maxsize=1,
    broker_commit_every=100,
    broker_commit_interval=1.0,
)

withdrawals_topic = app.topic('f-stress-withdrawals', value_type=Withdrawal)
seen_events = 0


@app.agent()
async def receiver(forwarded_stream):
    global seen_events
    total = 0
    async for withdrawal in forwarded_stream:
        seen_events += 1
        total += withdrawal.amount


@app.agent(withdrawals_topic)
async def withdrawal_forwarder(withdrawals):
    async for withdrawal in withdrawals:  # noqa
        await receiver.send(withdrawal)


@app.task
async def report_progress(app):
    prev_count = 0
    while not app.should_stop:
        await app._service.sleep(5.0)
        if seen_events <= prev_count:
            print(f'Forwarder not progressing: '
                  f'was {prev_count} now {seen_events}')
        else:
            print(f'Forwarder progressing: '
                  f'was {prev_count} now {seen_events}')
        prev_count = seen_events
