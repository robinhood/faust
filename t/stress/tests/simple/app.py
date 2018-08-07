from ...app import create_stress_app
from ...models import Withdrawal, generate_withdrawals

app = create_stress_app(
    name='f-stress-simple',
    version=7,
    origin='t.stress.tests.simple',
)

withdrawals_topic = app.topic('f-stress-withdrawals', value_type=Withdrawal)


@app.register_stress_producer
async def produce_withdrawals(max_messages):
    for withdrawal in generate_withdrawals(max_messages):
        yield await withdrawals_topic.send(key=None, value=withdrawal)


@app.agent(withdrawals_topic)
async def track_user_withdrawal(withdrawals):
    async for _withdrawal in withdrawals:  # noqa
        app.count_received_events += 1
