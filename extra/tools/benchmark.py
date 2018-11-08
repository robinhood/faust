import faust
from time import monotonic

app = faust.App(
    'benchmark',
    version=1,
    stream_buffer_maxsize=32_768,
)

benchmark_topic = app.topic('benchmark')


@app.agent(benchmark_topic)
async def process(stream):
    time_start = None
    async for i, value in stream.enumerate():
        if time_start is None:
            time_start = monotonic()
        if not i % 10_000:
            now = monotonic()
            print(f'TIME SPENT PROCESSING 10k: {now - time_start}')
            time_start = now


@app.command()
async def produce():
    for i in range(100_000):
        await process.send(value=i)


if __name__ == '__main__':
    import sys
    if len(sys.argv) == 1:
        sys.argv.extend(['worker', '-l', 'info'])
    app.main()
