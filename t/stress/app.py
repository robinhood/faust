from typing import Any, Callable, Iterator, List
import faust
from faust.cli import option
from faust.types import RecordMetadata
from faust.web import Request, Response, Web
from . import config
from . import producer

STATUS_OK = 'OK'
STATUS_SLOW = 'SLOW'
STATUS_STALL = 'STALL'
STATUS_UNASSIGNED = 'UNASSIGNED'

OK_STATUSES = {STATUS_OK, STATUS_UNASSIGNED}

__all__ = ['ProducerFun', 'StressApp', 'create_stress_app']

ProducerFun = Callable[[int], Iterator[RecordMetadata]]


class StressApp(faust.App):
    stress_producers: List[ProducerFun]
    count_received_events: int

    #: Status page reports this number.
    #: and report_progress background thread updates it as tests fail.
    faults: int = 0

    unassigned: bool = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stress_producers = []
        self.count_received_events = 0

    def register_stress_producer(self, fun: ProducerFun):
        self.stress_producers.append(fun)
        return fun


def faults_to_human_status(app):
    if app.unassigned:
        return STATUS_UNASSIGNED
    if app.faults > 6:
        return STATUS_STALL
    elif app.faults > 3:
        return STATUS_SLOW
    else:
        return STATUS_OK


def create_stress_app(name, origin, **kwargs: Any) -> StressApp:
    app = StressApp(
        name,
        origin=origin,
        broker=config.broker,
        store=config.store,
        topic_partitions=config.topic_partitions,
        loghandlers=config.loghandlers(),
        autodiscover=True,
        **kwargs)
    producer.install_produce_command(app)

    @app.task
    async def report_progress(app):
        prev_count = 0
        while not app.should_stop:
            severity = app.log.info
            await app._service.sleep(5.0)
            if not app.consumer.assignment():
                app.unassigned = True
                continue
            app.unassigned = False
            if app.count_received_events <= prev_count:
                app.faults += 1
                if app.faults > 6:
                    severity = app.log.error
                elif app.faults > 3:
                    severity = app.log.warn
                severity(f'{app.conf.id} not progressing (x{app.faults}): '
                         f'was {prev_count} now {app.count_received_events}')
            else:
                app.faults = 0
                severity(f'{app.conf.id} progressing: '
                         f'was {prev_count} now {app.count_received_events}')
            prev_count = app.count_received_events

    @app.page('/test/status/')
    async def get_status(web: Web, request: Request) -> Response:
        return web.json(
            {'status': faults_to_human_status(app), 'faults': app.faults},
        )

    @app.command(
        option('--host', type=str, default='localhost'),
        option('--port', type=int, default=6066),
        option('--description', type=str, default=''),
    )
    async def status(self, host: str, port: int, description: str):
        async with app.http_client as client:
            async with client.get(f'http://{host}:{port}/test/status/') as r:
                content = await r.json()
                status = content['status']
                color = 'green' if status in OK_STATUSES else 'red'
                print(f'{description}{self.color(color, status)}')

    return app
