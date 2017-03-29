import asyncio
import faust
from time import monotonic
from uuid import uuid4

faust.use_uvloop()
group = str(uuid4())
app = faust.App('faustbench-{}'.format(group))


class Request(faust.Record, serializer='json'):
    id: str
    time_start: float


topic = str(uuid4())
request_topic = faust.topic(topic, value_type=Request)


async def send_requests(app, n=1000):
    while 1:
        time_start = monotonic()
        for i in range(n):
            await app.send(request_topic, key=None, value=Request(
                id=str(uuid4()),
                time_start=monotonic(),
            ), wait=True)
        print('PRODUCED {}: {}'.format(n, monotonic() - time_start))
        asyncio.sleep(1)


async def process_requests(app, n=1000):
    i, time_start = 0, None
    s = app.stream(request_topic)
    async for request in s:
        i += 1
        if not i % n:
            if time_start is None:
                time_start = monotonic()
            else:
                print('CONSUMED {}: {}'.format(n, monotonic() - time_start))
                time_start = monotonic()


async def main():
    app.add_task(send_requests(app))
    app.add_task(process_requests(app))


if __name__ == '__main__':
    worker = faust.Worker(app, loglevel='INFO', debug=True)
    worker.execute_from_commandline(main())
