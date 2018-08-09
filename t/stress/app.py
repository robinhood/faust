from typing import Any, Callable, Iterator, List
from mode.utils.objects import cached_property
import faust
from faust.types import RecordMetadata
from . import config
from . import producer
from .reports.checks import Check, SystemChecks
from .reports.logging import LogHandler, LogPusher

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
        loghandler = LogHandler(self)
        kwargs['loghandlers'] = [loghandler]
        super().__init__(*args, **kwargs)
        self.stress_producers = []
        self.count_received_events = 0

    async def on_start(self) -> None:
        from .reports import checks
        self.system_checks.add(
            checks.Increasing(
                'events_total',
                get_value=lambda: self.monitor.events_total,
            ),
        )
        await self.add_runtime_dependency(self.system_checks)
        await self.add_runtime_dependency(self.logpusher)

    def add_system_check(self, check: Check) -> None:
        self.system_checks.add(check)

    def register_stress_producer(self, fun: ProducerFun):
        self.stress_producers.append(fun)
        return fun

    @cached_property
    def system_checks(self) -> SystemChecks:
        return SystemChecks(self)

    @cached_property
    def logpusher(self) -> LogPusher:
        return LogPusher(self)


def create_app(name, origin, base=faust.App, **kwargs: Any) -> faust.App:
    return base(
        name,
        origin=origin,
        broker=config.broker,
        store=config.store,
        topic_partitions=config.topic_partitions,
        loghandlers=config.loghandlers(),
        autodiscover=True,
        **kwargs)


def create_stress_app(name, origin, **kwargs: Any) -> StressApp:
    app = create_app(name, origin, base=StressApp, **kwargs)
    producer.install_produce_command(app)
    return app
