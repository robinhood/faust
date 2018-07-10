import random
from collections import Counter
from faust import Stream
from ...reports import checks
from ...app import create_stress_app

keys_set = 0
found_duplicates = 0
found_gaps = 0

app = create_stress_app(
    name='f-stress-tables',
    version=5,
    origin='t.stress.tests.tables',
    stream_wait_empty=False,
    broker_commit_every=100,
)

app.add_system_check(
    checks.Increasing(
        'keys_set',
        get_value=lambda: keys_set,
    ),
)
app.add_system_check(
    checks.Stationary(
        'table-duplicates',
        get_value=lambda: found_duplicates,
    ),
)
app.add_system_check(
    checks.Stationary(
        'table-gaps',
        get_value=lambda: found_gaps,
    ),
)

partitions_sent_counter = Counter()

table = app.Table('counted', key_type=int, value_type=int)


@app.task
async def on_leader_send_monotonic_counter(app, max_latency=0.08) -> None:
    # Leader node sends incrementing numbers to a topic

    partitions_sent_counter.clear()

    while not app.should_stop:
        if app.rebalancing:
            partitions_sent_counter.clear()
            await app._service.sleep(5)
        if app.is_leader():
            for partition in range(app.conf.topic_partitions):
                if app.rebalancing:
                    break
                current_value = partitions_sent_counter.get(partition, 0)
                await process.send(value=current_value, partition=partition)
                partitions_sent_counter[partition] += 1
            await app._service.sleep(random.uniform(0, max_latency))
        else:
            await app._service.sleep(1)


@app.agent(value_type=int)
async def process(numbers: Stream[int]) -> None:
    # Next agent reads from topic and forwards numbers to another agent
    # We keep the partitioning
    global keys_set
    global found_duplicates
    global found_gaps
    async for event in numbers.events():
        number = event.value
        assert isinstance(number, int)
        partition = event.message.partition
        previous_number = table.get(partition)
        if previous_number is not None:
            if number > 0:
                # consider 0 as the service being restarted.

                if number <= previous_number:
                    # number should be larger than the previous number.
                    # if that's not true it means we have a duplicate.
                    app.log.error('Found duplicate number in %r: %r',
                                  event.message.tp, number)
                    found_duplicates += 1
                else:
                    if number != previous_number + 1:
                        # the number should always be exactly one up from the
                        # last, otherwise we dropped a number.
                        app.log.error(
                            'Sequence gap for tp %r: this=%r previous=%r',
                            event.message.tp, number, previous_number)
                        found_gaps += 1
        table[partition] = number
        keys_set += 1
