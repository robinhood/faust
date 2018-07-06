import asyncio
import random
from faust import Stream
from faust.sensors import checks
from ...app import create_stress_app

counter_received = 0

app = create_stress_app(
    name='f-stress-duplicates',
    origin='t.stress.tests.forwarder',
    stream_wait_empty=False,
    broker_commit_every=100,
)

app.add_system_check(
    checks.Increasing(
        'counter_received',
        get_value=lambda: counter_received,
    ),
)


@app.task
async def on_leader_send_monotonic_counter(app, max_latency=0.01) -> None:
    # Leader node sends incrementing numbers to a topic
    counter_sent = 0

    while not app.should_stop:
        if app.is_leader():
            await forward.send(value=counter_sent)
            counter_sent += 1
            await asyncio.sleep(random.uniform(0, max_latency))
        else:
            await asyncio.sleep(1)


@app.agent(value_type=int)
async def forward(numbers: Stream[int]) -> None:
    # Next agent reads from topic and forwards numbers to another agent
    async for number in numbers:
        await receive.send(value=number)


@app.agent(value_type=int)
async def receive(forwarded_numbers: Stream[int]) -> None:
    # last agent recveices number and verifies numbers are always increasing.
    # (repeating or decreasing numbers are evidence of duplicates).
    global counter_received
    previous_number = None
    async for number in forwarded_numbers:
        assert isinstance(number, int)
        if previous_number is not None:
            if number > 0:
                # consider 0 as the service being restarted.
                assert number > previous_number
        previous_number = number
        counter_received += 1
