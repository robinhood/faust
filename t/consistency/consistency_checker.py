import asyncio
import json
from collections import defaultdict
from aiokafka import AIOKafkaConsumer


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
                loop=self.loop,
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
        k = json.loads(message.key.decode())
        v = json.loads(message.value.decode())
        self.table[k] = v
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
        self.loop = loop
        self._source_builder = SourceTableBuilder(source, loop)
        self.changelog = changelog
        self._changelog_builder = ChangelogTableBuilder(changelog, loop)

    async def check_consistency(self):
        await asyncio.wait(
            [self._source_builder.build(), self._changelog_builder.build()],
            loop=self.loop, return_when=asyncio.ALL_COMPLETED,
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
