import asyncio
import faust
from time import monotonic
from uuid import uuid4
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

app = faust.App('faustbench')


class Request(faust.Record, serializer='json'):
    id: str
    time_start: float


request_topic = faust.topic('request', value_type=Request)


async def send_requests(app):
    while 1:
        await app.send(request_topic, key=b'a', value=Request(
            id=str(uuid4()),
            time_start=monotonic(),
        ), wait=True)
        asyncio.sleep(1)


async def process_requests(app):
    s = app.stream(request_topic)
    async for request in s:
        print('Request {!r}: {}'.format(
            request.id, monotonic() - request.time_start))


async def main():
    app.add_task(send_requests(app))
    app.add_task(process_requests(app))


if __name__ == '__main__':
    worker = faust.Worker(app, loglevel='INFO')
    worker.execute_from_commandline(main())
