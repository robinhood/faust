import asyncio

_get_ev = asyncio.get_event_loop
# Show where loop is first created to make sure we don't create
# the event loop at module level when an app is created.


def xxx():
    print('-------- GET EVENT LOOP ----')
    import traceback
    traceback.print_stack()
    asyncio.get_event_loop = _get_ev
    return _get_ev()


asyncio.get_event_loop = xxx
import faust  # noqa
from time import monotonic  # noqa
from uuid import uuid4      # noqa

group = str(uuid4())
app = faust.App(f'faustbench-{group}', url='aiokafka://localhost')


class Request(faust.Record, serializer='json'):
    id: str
    time_start: float


topic = str(uuid4())
request_topic = app.topic(topic, value_type=Request)


async def send_requests(app, n=1000):
    for _ in range(10):
        time_start = monotonic()
        for _ in range(n):
            await app.send(request_topic, key=None, value=Request(
                id=str(uuid4()),
                time_start=monotonic(),
            ))
        time_end = monotonic() - time_start
        print(f'PRODUCED {n}: {time_end}')


@app.actor(request_topic)
async def process_requests(requests, n=1000):
    i, j, time_start = 0, 0, None
    async for i, request in requests.enumerate():
        i += 1
        if time_start is None:
            time_start = monotonic()
        assert request.id
        if not i % n:
            time_end = monotonic() - time_start
            print(f'CONSUMED {n}: {time_end}')
            time_start = monotonic()
            j += 1
            if j > 10:
                break


async def main():
    await send_requests(app)


if __name__ == '__main__':
    worker = faust.Worker(app, loglevel='INFO')
    worker.execute_from_commandline(main())
