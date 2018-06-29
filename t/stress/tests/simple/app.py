from ...app import create_stress_app
from ...models import Withdrawal, generate_withdrawals

app = create_stress_app(
    name='f-stress-simple',
    origin='t.stress.tests.simple',
)

withdrawals_topic = app.topic('f-stress-withdrawals', value_type=Withdrawal)
seen_events = 0


@app.register_stress_producer
async def produce_withdrawals(max_messages):
    for withdrawal in generate_withdrawals(max_messages):
        yield await withdrawals_topic.send(key=None, value=withdrawal)


@app.agent(withdrawals_topic)
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
