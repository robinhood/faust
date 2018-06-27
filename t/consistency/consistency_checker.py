import asyncio
from collections import defaultdict
from aiokafka import AIOKafkaClient, AIOKafkaConsumer
from faust.utils import json
from rhkafka.protocol.commit import (
    GroupCoordinatorRequest_v0, OffsetFetchRequest_v1,
)
from rhkafka.structs import TopicPartition


class MissingDataException(Exception):
    pass


bootstrap_servers = 'localhost:9092'


class BaseKafkaTableBuilder(object):
    '''
    Builds table using single consumer consuming linearly
    from raw topic
    '''

    def __init__(self, topic, loop):
        self.topic = topic
        self.consumer = None
        self.messages = []
        self.loop = loop
        self.table = defaultdict(int)
        self.key_tps = defaultdict(set)
        self._assignment = None

    async def build(self):
        await self._init_consumer()
        await self._build_table()

    def get_key(self, message):
        return json.loads(message.key.decode())

    def get_value(self, message):
        return json.loads(message.value.decode())

    async def _init_consumer(self):
        if not self.consumer:
            self.consumer = AIOKafkaConsumer(
                self.topic,
                loop=self.loop,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
            )
            await self.consumer.start()
            self._assignment = self.consumer.assignment()

    async def _build_table(self):
        while True:
            message = await self.consumer.getone()
            self.messages.append(message)
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
        k = self.get_key(message)
        v = self.get_value(message)
        self.table[k] = v
        self.key_tps[k].add(message.partition)


class SourceTableBuilder(BaseKafkaTableBuilder):

    async def _apply(self, message):
        k = self.get_key(message)
        v = self.get_value(message)
        self.table[k] += v['amount']
        self.key_tps[k].add(message.partition)


class ConsistencyChecker(object):

    def __init__(self, source, changelog, loop):
        self.source = source
        self.loop = loop
        self.client = AIOKafkaClient(bootstrap_servers=bootstrap_servers,
                                     loop=self.loop)
        self._source_builder = SourceTableBuilder(source, loop)
        self.changelog = changelog
        self._changelog_builder = ChangelogTableBuilder(changelog, loop)

    async def build_source(self):
        await self._source_builder.build()

    async def build_changelog(self):
        await self._changelog_builder.build()

    async def wait_no_lag(self):
        print('Ensuring no lag')
        consumer_group = 'f-simple'
        client = self.client
        await self.client.bootstrap()
        source = self.source
        source_builder = self._source_builder

        source_highwaters = source_builder._highwaters()
        source_tps = source_builder._assignment
        protocol_tps = [(source, [tp.partition for tp in source_tps])]

        node_id = next(broker.nodeId for broker in client.cluster.brokers())
        coordinator_request = GroupCoordinatorRequest_v0(consumer_group)
        coordinator_response = await client.send(node_id, coordinator_request)
        coordinator_id = coordinator_response.coordinator_id

        while True:
            consumer_offsets_req = OffsetFetchRequest_v1(
                consumer_group, protocol_tps)
            consumer_offsets_resp = await client.send(
                coordinator_id, consumer_offsets_req)
            topics = consumer_offsets_resp.topics
            assert len(topics) == 1, f'{topics!r}'
            topic, partition_resps = topics[0]
            assert topic == source, f'{source}'
            assert len(partition_resps) == len(source_tps)

            # + 1 is to account for the difference in how faust commits
            positions = {
                TopicPartition(topic=source, partition=partition): offset + 1
                for partition, offset, _, _ in partition_resps
            }

            if positions != source_highwaters:
                print('There is lag. Waiting!')
                await asyncio.sleep(2.0)
            else:
                return

    async def check_consistency(self):
        self._analyze()
        self._assert_results()

    def _analyze(self):
        res = self._changelog_builder.table
        truth = self._source_builder.table
        print('Res: {} keys | Truth: {} keys'.format(len(res), len(truth)))
        print('Res keys subset of truth: {}'.format(set(res).issubset(truth)))

        self._analyze_keys()
        self._analyze_key_partitions()

    def _analyze_keys(self):
        res = self._changelog_builder.table
        truth = self._source_builder.table

        # Analyze keys
        keys_same = True
        for key in res:
            if truth[key] != res[key]:
                keys_same = False
                break
        print('Keys in res have the same value in truth: {}'.format(keys_same))
        diff_keys = [k for k in res if truth[k] != res[k]]
        print('{} differing keys'.format(len(diff_keys)))

        for key in diff_keys:
            self._analyze_non_atomic_commit(key)

    def _get_messages_for_key(self, key):
        source_messages = [message for message in self._source_builder.messages
                           if self._source_builder.get_key(message) == key]
        cl_messages = [message for message in self._changelog_builder.messages
                       if self._changelog_builder.get_key(message) == key]
        return source_messages, cl_messages

    def _analyze_non_atomic_commit(self, key):
        print('Analyzing key: {}'.format(key))
        source_messages, cl_messages = self._get_messages_for_key(key)
        print('# source messages: {} # cl messags: {}'.format(len(
            source_messages), len(cl_messages)))
        cl_pos = source_sum = 0
        for i, message in enumerate(source_messages):
            source_sum += self._source_builder.get_value(message)['amount']
            while True:
                cl_sum = self._changelog_builder.get_value(cl_messages[cl_pos])
                if cl_sum > source_sum:
                    print('Key diverged at: source: {}, changelog: '
                          '{}'.format(i, cl_pos))
                    return
                elif cl_sum == source_sum:
                    break
                cl_pos += 1
                if cl_pos >= len(cl_messages):
                    raise MissingDataException

    def _analyze_key_partitions(self):
        res_kps = self._changelog_builder.key_tps
        truth_kps = self._source_builder.key_tps

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
