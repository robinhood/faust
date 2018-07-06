from typing import Any, Callable, Iterator, List
from mode.utils.objects import cached_property
import faust
from faust.cli import option
from faust.sensors import checks
from faust.types import RecordMetadata
from faust.types.sensors import SystemCheckT, SystemChecksT
from faust.web import Request, Response, Web
from . import config
from . import producer

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

    async def on_start(self) -> None:
        self.system_checks.add(
            checks.Increasing(
                'events_total',
                get_value=lambda: self.monitor.events_total,
            ),
        )
        await self.add_runtime_dependency(self.system_checks)

    def add_system_check(self, check: SystemCheckT) -> None:
        self.system_checks.add(check)

    def register_stress_producer(self, fun: ProducerFun):
        self.stress_producers.append(fun)
        return fun

    @cached_property
    def system_checks(self) -> SystemChecksT:
        return checks.SystemChecks(self)


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

    @app.page('/test/status/')
    async def get_status(web: Web, request: Request) -> Response:
        return web.json({
            'status': {
                check.name: check.asdict()
                for check in app.system_checks.checks.values()
            },
        })

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
                for check_name, info in status.items():
                    state = info['state']
                    color = info['color']
                    print(f'{description} {check_name}: '
                          f'{self.color(color, state)}')

    return app
