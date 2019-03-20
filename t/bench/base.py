import asyncio
import os
import random
import sys
from itertools import count
from time import monotonic
from faust.cli import option

__all__ = ['Benchmark']

TIME_EVERY = 10_000
BENCH_TYPE = os.environ.get('F_BENCH', 'worker')
ACKS = int(os.environ.get('F_ACKS') or 0)
BSIZE = int(os.environ.get('F_BSIZE', 16384))
LINGER = int(os.environ.get('F_LINGER', 0))


class Benchmark:
    _agent = None
    _produce = None

    produce_options = [
        option('--max-latency',
               type=float, default=0.0, envvar='PRODUCE_LATENCY',
               help='Add delay of (at most) n seconds between publishing.'),
        option('--max-messages',
               type=int, default=None,
               help='Send at most N messages or 0 for infinity.'),
    ]

    def __init__(self, app, topic, *,
                 n=TIME_EVERY,
                 consume_topic=None):
        self.app = app
        self.topic = topic
        if consume_topic is None:
            consume_topic = topic
        self.consume_topic = consume_topic
        self.n = n

        self.app.finalize()
        self.app.conf.producer_acks = ACKS
        self.app.conf.producer_max_batch_size = BSIZE
        self.app.conf.producer_linger_ms = LINGER

    def install(self, main_name):
        self.create_benchmark_agent()
        self.create_produce_command()
        if main_name == '__main__':
            # we use this for cProfile
            bench_args = {
                'worker': ['worker', '-l', 'info'],
                'produce': ['produce'],
            }
            if len(sys.argv) < 2:
                sys.argv.extend(bench_args[BENCH_TYPE])
            self.app.main()

    def create_benchmark_agent(self):
        self._agent = self.app.agent(self.consume_topic)(self.process)

    async def process(self, stream):
        time_last = None
        async for i, value in stream.enumerate():
            if not i:
                time_last = monotonic()
            await self.process_value(value)
            if i and not i % self.n:  # noqa: S001
                now = monotonic()
                runtime, time_last = now - time_last, now
                print(f'RECV {i} in {runtime}s')

    async def process_value(self, value):
        # put worker benchmark-specific code in here
        ...

    def create_produce_command(self):
        self._produce = self.app.command(*self.produce_options)(self.produce)

    async def produce(self, max_latency: float, max_messages: int, **kwargs):
        i = 0
        time_start = None
        app = self.app
        topic = self.topic
        for i, (key, value) in enumerate(self.generate_values(max_messages)):
            callback = None
            if not i:
                time_start = monotonic()
                time_1st = monotonic()

                def on_published(meta):
                    print(f'1ST OK: {meta} AFTER {monotonic() - time_1st}s')
                callback = on_published
            await topic.send(key=key, value=value, callback=callback)
            if i and not i % self.n:  # noqa: S001
                print(f'+SEND {i} in {monotonic() - time_start}s')
                time_start = monotonic()
            if max_latency:
                await asyncio.sleep(random.uniform(0, max_latency))
        await asyncio.sleep(10)
        await app.producer.stop()
        print(f'Time spent on {i} messages: {monotonic() - time_start}s')

    def generate_values(self, max_messages: int):
        # override this to generate other types of values
        return (
            # convert int to bytes, cannot use bytes(int) as that gives
            # character code in Python3.
            (None, str(i).encode())
            for i in (range(max_messages) if max_messages else count())
        )
