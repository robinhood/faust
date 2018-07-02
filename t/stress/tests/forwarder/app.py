from faust import Stream
from ...app import create_stress_app

app = create_stress_app(
    name='f-stress-duplicates',
    origin='t.stress.tests.forwarder',
    stream_wait_empty=False,
    broker_commit_every=100,
)

counter_sent = 0
counter_received = 0


@app.timer(0.1, on_leader=True)
async def monotonic_counter() -> None:
    # Leader node sends incrementing numbers to a topic
    global counter_sent
    await forward.send(value=counter_sent)
    counter_sent += 1


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
        counter_received += 1


@app.task
async def report_progress(app):
    prev_count = 0
    while not app.should_stop:
        await app._service.sleep(5.0)
        if counter_received <= prev_count:
            print(f'Forwarder not progressing: '
                  f'was {prev_count} now {counter_received}')
        else:
            print(f'Forwarder progressing: '
                  f'was {prev_count} now {counter_received}')
        prev_count = counter_received
