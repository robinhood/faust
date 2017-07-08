import asyncio
import json
import random
import subprocess
from collections import defaultdict
from aiokafka import AIOKafkaConsumer


class Stresser(object):

    def __init__(self, num_workers, num_producers, loop):
        self.workers = set(range(num_workers))
        self._worker_procs = {}
        self.num_workers = num_workers
        self.num_producers = num_producers
        self.producers = set(range(num_producers))
        self._producer_procs = {}
        self.loop = loop

    @property
    def _stopped(self):
        return self.workers - self._running

    @property
    def _running(self):
        return set(self._worker_procs)

    @property
    def _stopped(self):
        return self.producers - self._running_producers

    @property
    def _running_producers(self):
        return set(self._producer_procs)

    async def _run_stresser(self):
        print('Running stresser')
        while True:
            await asyncio.sleep(random.uniform(5, 30))
            print('Stresser iteration')
            await self._maybe_stop_worker()
            await self._maybe_spawn_worker()

    @classmethod
    def _should(cls):
        return random.choices([True, False], [0.75, 0.25], k=1)[0]

    async def _maybe_stop_worker(self):
        print('Maybe stop')
        if self._should() and len(self._running) > 1:
            await self._stop_worker(random.choice(list(self._running)))

    async def _maybe_spawn_worker(self):
        print('Maybe start')
        if self._should() and self._stopped:
            await self._start_worker(random.choice(list(self._stopped)))

    async def start(self, stopped_at_start=0):
        assert stopped_at_start < self.num_workers
        start_workers = random.sample(list(self.workers),
                                      self.num_workers - stopped_at_start)
        print(f'Start workers: {start_workers!r}')
        await asyncio.wait([self._start_producer(producer)
                            for producer in self.producers])
        await asyncio.wait([self._start_worker(worker)
                            for worker in start_workers],
                           loop=self.loop, return_when=asyncio.ALL_COMPLETED)
        asyncio.ensure_future(self._run_stresser(), loop=loop)

    async def _start_worker(self, worker):
        assert worker in self.workers
        with open(f'worker_{worker}.logs', 'w') as f:
            if worker not in self._worker_procs:
                print(f'Starting worker {worker}')
                self._worker_procs[worker] = await asyncio.create_subprocess_exec(
                    'faust',
                    '-A', 'examples.simple',
                    'worker',
                    '-l', 'info',
                    '--web-port', str(8080 + worker),
                    stdout=f,
                    stderr=subprocess.STDOUT,
                )

    async def stop_all(self):
        await asyncio.wait(
            [self._stop_worker(worker) for worker in self._running],
            loop=self.loop, return_when=asyncio.ALL_COMPLETED,
        )

    async def stop_all_producers(self):
        await asyncio.wait(
            [self._stop_producer(producer)
             for producer in self._running_producers],
            loop=self.loop, return_when=asyncio.ALL_COMPLETED,
        )

    async def _stop_worker(self, worker):
        assert worker in self.workers
        print(f'Stopping worker {worker}')
        proc = self._worker_procs[worker]
        await self._stop_process(proc)
        del self._worker_procs[worker]

    async def _start_producer(self, producer):
        assert producer in self.producers
        if producer not in self._producer_procs:
            with open(f'producer_{producer}.logs', 'w') as f:
                print(f'Starting producer: {producer}')
                self._producer_procs[producer] = await asyncio.create_subprocess_exec(
                    'python',
                    '/Users/vineet/faust/examples/simple.py',
                    'produce',
                    '-l', 'info',
                    stdout=f,
                    stderr=subprocess.STDOUT,
                )

    async def _stop_producer(self, producer):
        assert producer in self.producers
        print(f'Stopping producer {producer}')
        proc = self._producer_procs[producer]
        await self._stop_process(proc)
        del self._producer_procs[producer]

    async def _stop_process(self, proc):
        try:
            proc.terminate()
        except ProcessLookupError:
            pass
        await proc.wait()


class BaseKafkaTableBuilder(object):
    '''
    Builds table using single consumer consuming linearly
    from raw topic
    '''

    def __init__(self, topic, loop):
        self.topic = topic
        self.consumer = None
        self.loop = loop
        self.table = defaultdict(int)
        self.key_tps = defaultdict(set)
        self._assignment = None

    async def build(self):
        await self._init_consumer()
        await self._build_table()

    async def _init_consumer(self):
        if not self.consumer:
            self.consumer = AIOKafkaConsumer(
                self.topic,
                loop=loop,
                bootstrap_servers='localhost:9092',
                auto_offset_reset='earliest',
            )
            await self.consumer.start()
            self._assignment = self.consumer.assignment()

    async def _build_table(self):
        while True:
            message = await self.consumer.getone()
            await self._apply(message)
            if await self._positions() == self._highwaters():
                print('Done building table')
                return

    async def _apply(self, message):
        print(message)

    async def _positions(self):
        assert self.consumer
        return {
            tp: await self.consumer.position(tp)
            for tp in self._assignment
        }

    def _highwaters(self):
        assert self.consumer
        return {
            tp: self.consumer.highwater(tp)
            for tp in self._assignment
        }


class ChangelogTableBuilder(BaseKafkaTableBuilder):

    async def _apply(self, message):
        k = json.loads(message.key)
        v = json.loads(message.value.decode())
        self.table[k] = v['value']
        self.key_tps[k].add(message.partition)


class SourceTableBuilder(BaseKafkaTableBuilder):

    async def _apply(self, message):
        k = message.key.decode()
        v = json.loads(message.value.decode())
        self.table[k] += v['amount']
        self.key_tps[k].add(message.partition)


class ConsistencyChecker(object):

    def __init__(self, source, changelog, loop):
        self.source = source
        self._source_builder = SourceTableBuilder(source, loop)
        self.changelog = changelog
        self._changelog_builder = ChangelogTableBuilder(changelog, loop)

    async def check_consistency(self):
        await asyncio.wait(
            [self._source_builder.build(), self._changelog_builder.build()],
            loop=loop, return_when=asyncio.ALL_COMPLETED,
        )
        self._analyze()
        self._assert_results()

    def _analyze(self):
        res = self._changelog_builder.table
        truth = self._source_builder.table
        res_kps = self._changelog_builder.key_tps
        truth_kps = self._source_builder.key_tps
        print('Res: {} keys | Truth: {} keys'.format(len(res), len(truth)))
        print('Res keys subset of truth: {}'.format(set(res).issubset(truth)))

        # Analyze keys
        keys_same = True
        for key in res:
            if truth[key] != res[key]:
                keys_same = False
                break
        print('Keys in res have the same value in truth: {}'.format(keys_same))
        diff_keys = [k for k in res if truth[k] != res[k]]
        print('{} differing keys'.format(len(diff_keys)))
        diff_factors = [(res[k] / truth[k]) for k in diff_keys]
        print('Differing factors: {}'.format(diff_factors))

        # Analyze key partitions
        print('Res kps and truth kps have same keys: {}'.format(
            res_kps.keys() == truth_kps.keys()))
        diff_kps = [(key, truth_kps[key], res_kps[key])
                    for key in truth_kps if truth_kps[key] != res_kps[key]]
        print('Key partitions are the same: {}'.format(not diff_kps))

    def _assert_results(self):
        res = self._changelog_builder.table
        truth = self._source_builder.table
        res_kps = self._changelog_builder.key_tps
        truth_kps = self._source_builder.key_tps
        assert res == truth, 'Tables need to be the same'
        assert res_kps == truth_kps, 'Must be co-located'


async def test_consistency(loop):
    stresser = Stresser(num_workers=4, num_producers=4, loop=loop)
    print('Starting stresser')
    await stresser.start(stopped_at_start=1)
    print('Waiting for stresser to run')
    await asyncio.sleep(120)  # seconds to run stresser for
    print('Stopping all producers')
    await stresser.stop_all_producers()
    print('Waiting for consumer lag to be 0')
    await asyncio.sleep(20)  # wait for consumer lag to reach 0
    print('Stopping everything')
    await stresser.stop_all()
    checker = ConsistencyChecker('withdrawals',
                                 'f-simple-user_to_total-changelog', loop=loop)
    await checker.check_consistency()



if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_consistency(loop))
