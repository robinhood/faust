#!/usr/bin/env python
import asyncio
import faust
import json

ITERATIONS = 10_000
EXPECTED_SUM = sum(range(ITERATIONS))

app = faust.App(
    'tabletest',
    broker='kafka://localhost:9092',
    store='rocksdb://',
    origin='examples.tabletest',
    version=1,
    topic_partitions=4,
    stream_buffer_maxsize=16368,
)

source_topic = app.topic('countstopic', key_type=str, value_type=int)
counts = app.Table('counts', default=int)
seen = {}
prev_offsets = {}


@app.agent(source_topic)
async def count(stream):
    async for event in stream.events():
        partition = event.key
        count = event.value
        prev = seen.get(partition)
        prev_offset = prev_offsets.get(partition)
        if prev is not None:
            if count != prev + 1:
                print(f'!!! PREV {partition} WAS {prev} NOW {count}')
                print(f'OFFSET: {event.message.offset} PREV: {prev_offset}')
                import time
                time.sleep(3600)
        seen[partition] = count
        prev_offsets[partition] = event.message.offset
        counts[str(partition)] += count


@app.page('/count/{partition}/')
@app.table_route(table=counts, match_info='partition')
async def get_count(web, request, partition):
    return web.json({
        partition: counts[str(partition)],
    })


@app.on_rebalance_complete.connect
async def on_rebalance_complete(sender, **kwargs):
    print(counts.as_ansitable(
        key='partition',
        value='count',
        title='$$ TALLY - after rebalance $$',
        sort=True,
    ))


@app.timer(10.0)
async def dump_count():
    if not app.rebalancing:
        print(counts.as_ansitable(
            key='partition',
            value='count',
            title='$$ TALLY $$',
            sort=True,
        ))


@app.command()
async def produce():
    for i in range(ITERATIONS):
        last_fut = None
        for j in range(app.conf.topic_partitions):
            last_fut = await source_topic.send(
                key=str(j), value=i, partition=j)
        if not i % 100:
            await last_fut  # wait for buffer to flush
            await asyncio.sleep(2.0)
            print(i)


@app.command()
async def add_single_changelog():
    topic = counts.changelog_topic
    await topic.send(key=0, value=json.dumps(0), partition=0)


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        sys.argv.extend(['worker', '-l', 'info'])
    app.main()
