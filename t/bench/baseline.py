import asyncio
import os
import random
from itertools import count
from time import monotonic
import faust
from faust.cli import option

TIME_EVERY = 10_000
BENCH_TYPE = os.environ.get('F_BENCH', 'worker')
ACKS = int(os.environ.get('F_ACKS') or 0)
BSIZE = int(os.environ.get('F_BSIZE', 16384))
LINGER = int(os.environ.get('F_LINGER', 0))

app = faust.App(
    'bench-baseline',
    producer_acks=ACKS,
    producer_max_batch_size=BSIZE,
    producer_linger_ms=LINGER,
)

topic = app.topic('bench-baseline', value_serializer='raw', value_type=int)


@app.agent(topic)
async def process(stream):
    time_last = None
    async for i, value in stream.enumerate():
        if not i:
            time_last = monotonic()
        if i and not i % TIME_EVERY:
                now = monotonic()
                runtime, time_last = now - time_last, now
                print(f'RECV {i} in {runtime}s')


@app.command(
    option('--max-latency',
           type=float, default=0.0, envvar='PRODUCE_LATENCY',
           help='Add delay of (at most) n seconds between publishing.'),
    option('--max-messages',
           type=int, default=None,
           help='Send at most N messages or 0 for infinity.'),
)
async def produce(self, max_latency: float, max_messages: int):
    """Produce example Withdrawal events."""
    i = 0
    time_start = None
    for i in range(max_messages) if max_messages else count():
        callback = None
        if not i:
            time_start = monotonic()
            time_1st = monotonic()

            def on_published(meta):
                print(f'1ST OK: {meta} AFTER {monotonic() - time_1st}s')
            callback = on_published
        await topic.send(key=None, value=str(i).encode(), callback=callback)
        if i and not i % TIME_EVERY:
            self.say(f'+SEND {i} in {monotonic() - time_start}s')
            time_start = monotonic()
        if max_latency:
            await asyncio.sleep(random.uniform(0, max_latency))
    await asyncio.sleep(10)
    await app.producer.stop()
    print(f'Time spent on {i} messages: {monotonic() - time_start}s')

if __name__ == '__main__':
    import sys
    bench_args = {
        'worker': ['worker', '-l', 'info'],
        'produce': ['produce'],
    }
    if len(sys.argv) < 2:
        sys.argv.extend(bench_args[BENCH_TYPE])
    app.main()
