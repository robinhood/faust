import asyncio
import random
from collections import Counter
from faust import Stream
from faust.sensors import checks
from ...app import create_stress_app

counter_received = 0
found_duplicate = 0

app = create_stress_app(
    name='f-stress-dedupe',
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
app.add_system_check(
    checks.Stationary(
        'duplicates',
        get_value=lambda: found_duplicate,
    ),
)

partitions = 100
partitions_sent_counter = Counter()


@app.task
async def on_leader_send_monotonic_counter(app, max_latency=0.08) -> None:
    # Leader node sends incrementing numbers to a topic

    partitions_sent_counter.clear()

    while not app.should_stop:
        if app.is_leader():
            for partition in range(partitions):
                current_value = partitions_sent_counter.get(partition, 0)
                await process.send(value=current_value, partition=partition)
                partitions_sent_counter[partition] += 1
            await asyncio.sleep(random.uniform(0, max_latency))
        else:
            await asyncio.sleep(1)


@app.agent(value_type=int)
async def process(numbers: Stream[int]) -> None:
    # Next agent reads from topic and forwards numbers to another agent
    async for number in numbers:
        await check.send(value=number)


@app.agent(value_type=int)
async def check(forwarded_numbers: Stream[int]) -> None:
    # last agent recveices number and verifies numbers are always increasing.
    # (repeating or decreasing numbers are evidence of duplicates).
    global counter_received
    global found_duplicates
    value_by_partition = Counter()
    async for event in forwarded_numbers.events():
        number = event.value
        assert isinstance(number, int)
        partition = event.message.partition
        assert isinstance(partition, int)
        previous_number = value_by_partition.get(partition, None)
        if previous_number is not None:
            if number > 0:
                # consider 0 as the service being restarted.
                if number <= previous_number:
                    app.log.error('Found duplicate number: %r', number)
                    found_duplicates += 1
        value_by_partition[partition] = number
        counter_received += 1
