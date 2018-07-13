import random
from collections import Counter
from copy import copy
from faust import Stream
from ...reports import checks
from ...app import create_stress_app

keys_set = 0
found_duplicates = 0
found_gaps = 0

app = create_stress_app(
    name='f-stress-tables',
    version=6,
    origin='t.stress.tests.tables',
    stream_wait_empty=False,
    broker_commit_every=100,
)

processed_total = 0
processed_by_partition = Counter()


class PartitionsNotStarved(checks.Check):
    description = 'fine'
    negate_description = 'starved'

    def compare(self, prev_value, current_value):
        # takes two collections.Counter as argument
        assert prev_value is not current_value

        # yeah, this is a hack, but we don't want to have 100 threads
        # checking this, and no time to make this better as of now. [ask]
        differing = {}
        for partition, prev_number in prev_value.items():
            current_number = current_value[partition]
            if current_value[partition] <= prev_number:
                differing[partition] = (prev_number, current_number)

        if differing:
            self.prev_value_repr = self.current_value_repr
            self.current_value_repr = repr(differing)
            if len(differing) == len(current_value):
                self.current_value_repr = 'all partitions starved'

        return bool(differing)

    def get_value(self):
        return processed_by_partition

    def store_previous_value(self, current_value):
        self.prev_value = copy(current_value)


app.add_system_check(
    PartitionsNotStarved('starved-partitions'),
)
app.add_system_check(
    checks.Stationary(
        'table-found-double-count',
        get_value=lambda: found_duplicates,
    ),
)
app.add_system_check(
    checks.Stationary(
        'table-found-missing-update',
        get_value=lambda: found_gaps,
    ),
)

partitions_sent_counter = Counter()

table = app.Table('counted', key_type=int, value_type=int)


@app.task
async def on_leader_send_monotonic_counter(app, max_latency=0.01) -> None:
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
    global processed_total

    # This is a stream of numbers where every partition count from 1 to
    # infinity.

    # If we have processed this stream correctly
    #   - there shouldn't be any duplicate numbers
    #   - there shouldn't be any missing numbers.

    async for event in numbers.events():
        number = event.value
        assert isinstance(number, int)
        partition = event.message.partition
        previous_number = table.get(partition)
        if processed_total and not processed_total % 30_000:
            with app.system_checks.pause():
                print('Pausing stream to fill topics')
                await app._service.sleep(100.0)

        if previous_number is not None:
            if number > 0:
                # 0 is fine, means leader restarted.

                if number <= previous_number:
                    # Remember, the key is the partition.
                    # So the previous value set for any partition,
                    # should always be the same as the previous value in the
                    # topic.
                    #
                    # if number is less than previous number, it means
                    # we have missed counting one (the sequence counts
                    # WITH THE OFFSET after all!)
                    # if the number is less than we have a problem.
                    app.log.error('Found duplicate number in %r: %r',
                                  event.message.tp, number)
                    found_duplicates += 1
                else:
                    if number != previous_number + 1:
                        # number should always be one up from the last,
                        # otherwise we dropped a number.
                        app.log.error(
                            'Sequence gap for tp %r: this=%r previous=%r',
                            event.message.tp, number, previous_number)
                        found_gaps += 1
        table[partition] = number
        processed_by_partition[partition] += 1
        processed_total += 1
