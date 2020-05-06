#!/usr/bin/env python
import asyncio
import random
import time
from itertools import count
import faust
from faust.cli import option
from faust.utils.kafka.tools import kafka_console_consume
from mode.utils.futures import notify
from mode.utils.logging import get_logger

VERSION = 15
TOPIC_PARTITIONS = 1
logger = get_logger(__name__)


def _generate_generation() -> int:
    return time.strftime('%Y%m%d%H%M%S')


GENERATION = _generate_generation()


class OffsetInfo(faust.Record):
    generation: int
    offset: int
    end_offset: int


app = faust.App(
    'faust-integrity-commit',
    version=VERSION,
    debug=True,
    broker='aiokafka://',
    store='rocksdb://',
    origin='examples.withdrawals',
    topic_partitions=TOPIC_PARTITIONS,
    broker_max_poll_interval=1.0,
    # processing_guarantee='exactly_once',
)
offsets_topic = app.topic(
    f'faust-integrity-commit-offsets{VERSION}',
    value_type=OffsetInfo,
)
receipts_topic = app.topic(
    f'faust-integrity-commit-processed{VERSION}',
    value_type=OffsetInfo,
)

offsets_table = app.Table('integrity-commit-offsets', value_type=OffsetInfo)

@app.agent(offsets_topic, isolated_partitions=True)
async def process_offsets(offsets):
    previous_generation: int = None
    previous_offset: int = None
    async for event in offsets.events():
        offsetinfo = event.value
        print('PROCESS OFFSET %r' % (offsetinfo,))
        generation = offsetinfo.generation
        partition = event.message.partition
        end_offset = offsetinfo.end_offset
        offset = offsetinfo.offset
        #print('OFFSET %r MSG=%r FIN=%r' % (offset, event.message.offset,
        #    finalize_partition))
        if previous_generation is not None:
            assert generation == previous_generation
        if previous_offset is not None:
            assert offset == previous_offset + 1, (offset, previous_offset)

        await process_receipts.send(
            value=event.value,
            partition=partition,
        )

        if offset >= 1033:
            print('--------- MARK -----------')
            for i, o in enumerate(await kafka_console_consume(
                    'localhost', 9092,
                    offsets_topic.get_topic_name(), partition)):
                o2 = OffsetInfo.loads(o, serializer='json')
                assert o2.generation == generation
                assert o2.offset == i
            raise SystemExit(0)

        if offset >= end_offset:
            print('SENDING FINALIZE RECEIPT FOR OFFSET %r %r' % (
                offset, partition,))
            await process_offset_request.send(
                value=OffsetRequest(
                    generation=generation,
                    start=0,
                    end=offset_end,
                    partition=partition,
                    finalized=True,
                ),
                partition=partition)
        else:
            await process_offsets.send(
                value=offsetinfo.derive(offset=offset + 1),
                partition=partition,
            )

        previous_generation = generation
        previous_offset = offset



class OffsetRequest(faust.Record):
    generation: int
    start: int
    end: int
    partition: int
    finalized: bool = False


@app.agent()
async def process_offset_request(stream):
    async for offset_request in stream:
        print('+PROCESS OFFSET REQUEST %r' % (offset_request,))
        if offset_request.finalized:
            print('FINALIZE RECEIPT: %r' % (offset_request,))
            logger.info('Processed all of %r', offset_request)
            continue

        generation = offset_request.generation
        range_start = offset_request.start
        range_end = offset_request.end
        partition = offset_request.partition

        print('SEND %r %r-%r' % (generation, range_start, range_end))
        await process_offsets.send(
            value=OffsetInfo(generation, range_start, range_end),
            partition=partition,
        )
        print('-PROCESS OFFSET REQUEST %r' % (offset_request,))



@app.agent(receipts_topic)
async def process_receipts(offsets):
    async for event in offsets.events():
        info = event.value
        partition = event.message.partition
        key = f'{partition}-{info.generation}-{info.offset}'

        current_value = offsets_table.get(key)
        if current_value:
            print(f'Already processed for key {key!r}')
        offsets_table[key] = info


@app.command(
    option('--max-latency',
           type=float, default=0.5, envvar='PRODUCE_LATENCY',
           help='Add delay of (at most) n seconds between publishing.'),
    option('--max-messages',
           type=int, default=None,
           help='Send at most N messages or 0 for infinity.'),
    option('--partition',
           type=int, default=None,
           help='Partition number to send for (or None for all)'),
    option('--generation',
           type=int, default=None,
           help='Specify generation number to use.'),
    option('--offset-start',
           type=int, default=0,
           help='Starting offset.'),
)
async def produce(self,
                  max_latency: float,
                  max_messages: int,
                  partition: int,
                  generation: int,
                  offset_start: int):
    """Produce example Withdrawal events."""
    batchsize = 1000
    generation = generation or GENERATION
    offset_start = offset_start or 0
    print(f'Generation={generation} latency={max_latency} n={max_messages}')

    async def send_for_offsets(start: int, end: int,
                               partition: int = None) -> None:
        if partition is None:
            for partition in range(TOPIC_PARTITIONS):
                await _send_for_offsets(partition, start, end)
        else:
            await _send_for_offsets(partition, start, end)
        self.say(f'+SEND {offset_start}-{offset_end}')

    async def _send_for_offsets(partition: int, start: int, end: int) -> None:
        await process_offset_request.send(
            value=OffsetRequest(
                generation=generation,
                start=start,
                end=end,
                partition=partition,
            ),
            partition=partition,
        )


    if offset_start:
        offset_end = offset_start + max_messages or 1000
        return await send_for_offsets(
            offset_start, offset_end,
            partition=partition,
        )
    else:
        offset_start = 0
        offset_end = 100_000
        await send_for_offsets(
            offset_start, offset_end,
            partition=partition,
        )


if __name__ == '__main__':
    app.main()


