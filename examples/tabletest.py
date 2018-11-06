#!/usr/bin/env python
import asyncio
import faust

app = faust.App(
    'tabletest',
    broker='kafka://localhost:9092',
    store='rocksdb://',
    version=2,
    topic_partitions=8,
)

counts = app.Table('counts', default=int)


@app.agent(key_type=str, value_type=int)
async def count(stream):
    async for partition, count in stream.items():
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
    for i in range(10000):
        last_fut = None
        for j in range(app.conf.topic_partitions):
            last_fut = await count.send(
                key=str(j), value=i, partition=j)
        if not i % 100:
            await last_fut  # wait for buffer to flush
            await asyncio.sleep(2.0)
            print(i)


if __name__ == '__main__':
    app.main()
